/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.Term;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.VersionType;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.indices.pollingingest.DefaultStreamPoller;
import org.opensearch.indices.pollingingest.IngestionErrorStrategy;
import org.opensearch.indices.pollingingest.IngestionSettings;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.indices.pollingingest.StreamPoller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_LOCATION;
import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_SNAPSHOT;

/**
 * IngestionEngine is an engine that ingests data from a stream source.
 */
public class IngestionEngine extends InternalEngine {

    private StreamPoller streamPoller;
    private final IngestionConsumerFactory ingestionConsumerFactory;
    private final DocumentMapperForType documentMapperForType;
    private volatile IngestionShardPointer lastCommittedBatchStartPointer;

    /**
     * Tracks the minimum successful pointer at the time of the last internal refresh.
     * Used for partial update refresh optimization - if the current minimum successful pointer
     * is greater than this value and the version map has an entry for a document,
     * we need to refresh to see the latest data for that document.
     */
    private volatile IngestionShardPointer lastRefreshedSuccessfulPointer;

    public IngestionEngine(EngineConfig engineConfig, IngestionConsumerFactory ingestionConsumerFactory) {
        super(engineConfig);
        this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);
        this.documentMapperForType = engineConfig.getDocumentMapperForTypeSupplier().get();
        registerDynamicIndexSettingsHandlers();
    }

    /**
     * Starts the ingestion engine to pull.
     */
    public void start() {
        initializeStreamPoller(null, null, null);
    }

    private void initializeStreamPoller(
        @Nullable StreamPoller.ResetState resetStateOverride,
        @Nullable String resetValueOverride,
        @Nullable IngestionShardPointer startPointerOverride
    ) {
        IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
        assert indexMetadata != null;
        IngestionSource ingestionSource = Objects.requireNonNull(indexMetadata.getIngestionSource());

        // initialize the ingestion consumer factory
        this.ingestionConsumerFactory.initialize(ingestionSource);
        String clientId = engineConfig.getIndexSettings().getNodeName()
            + "-"
            + engineConfig.getIndexSettings().getIndex().getName()
            + "-"
            + engineConfig.getShardId().getId();

        Map<String, String> commitData = commitDataAsMap(documentIndexWriter);
        StreamPoller.ResetState resetState = ingestionSource.getPointerInitReset().getType();
        String resetValue = ingestionSource.getPointerInitReset().getValue();
        IngestionShardPointer startPointer = null;
        boolean forceResetPoller = resetStateOverride != null
            && Strings.isNullOrEmpty(resetValueOverride) == false
            && startPointerOverride != null;

        // initialize ingestion start pointer
        if (commitData.containsKey(StreamPoller.BATCH_START) || forceResetPoller) {
            if (forceResetPoller) {
                startPointer = startPointerOverride;
            } else {
                // try recovering from commit data
                String batchStartStr = commitData.get(StreamPoller.BATCH_START);
                startPointer = this.ingestionConsumerFactory.parsePointerFromString(batchStartStr);

                // reset to none so the poller will poll from the startPointer
                resetState = StreamPoller.ResetState.NONE;
            }
        }

        if (forceResetPoller) {
            resetState = resetStateOverride;
            resetValue = resetValueOverride;
        }

        IngestionErrorStrategy ingestionErrorStrategy = IngestionErrorStrategy.create(
            ingestionSource.getErrorStrategy(),
            ingestionSource.getType()
        );

        StreamPoller.State initialPollerState = indexMetadata.getIngestionStatus().isPaused()
            ? StreamPoller.State.PAUSED
            : StreamPoller.State.NONE;

        // initialize the stream poller
        DefaultStreamPoller.Builder streamPollerBuilder = new DefaultStreamPoller.Builder(
            startPointer,
            ingestionConsumerFactory,
            clientId,
            engineConfig.getShardId().getId(),
            this
        );
        streamPoller = streamPollerBuilder.resetState(resetState)
            .resetValue(resetValue)
            .errorStrategy(ingestionErrorStrategy)
            .initialState(initialPollerState)
            .maxPollSize(ingestionSource.getMaxPollSize())
            .pollTimeout(ingestionSource.getPollTimeout())
            .numProcessorThreads(ingestionSource.getNumProcessorThreads())
            .blockingQueueSize(ingestionSource.getBlockingQueueSize())
            .pointerBasedLagUpdateInterval(ingestionSource.getPointerBasedLagUpdateInterval().millis())
            .mapperType(ingestionSource.getMapperType())
            .build();
        registerStreamPollerListener();

        // start the polling loop
        streamPoller.start();
    }

    private void registerStreamPollerListener() {
        // Register the poller with the ClusterService for receiving cluster state updates.
        // Also initialize cluster write block state in the poller.
        if (engineConfig.getClusterApplierService() != null) {
            engineConfig.getClusterApplierService().addListener(streamPoller);
            boolean isWriteBlockEnabled = engineConfig.getClusterApplierService()
                .state()
                .blocks()
                .indexBlocked(ClusterBlockLevel.WRITE, engineConfig.getIndexSettings().getIndex().getName());
            streamPoller.setWriteBlockEnabled(isWriteBlockEnabled);
        }

        // Register listener for dynamic ingestion source params updates
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addAffixMapUpdateConsumer(IndexMetadata.INGESTION_SOURCE_PARAMS_SETTING, this::updateIngestionSourceParams, (x, y) -> {});
    }

    private void unregisterStreamPollerListener() {
        if (engineConfig.getClusterApplierService() != null) {
            engineConfig.getClusterApplierService().removeListener(streamPoller);
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        throw new IngestionEngineException("push-based indexing is not supported in ingestion engine, use streaming source instead");
    }

    /**
     * Indexes the document into the engine. This is used internally by the stream poller only.
     * @param index the index request
     * @param isCreateMode if true, a new document is created. Existing document with same docID will not be updated.
     * @throws IOException if an error occurs
     */
    public void indexInternal(Index index, boolean isCreateMode) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();

        try (
            ReleasableLock releasableLock1 = readLock.acquire();
            Releasable releasableLock2 = versionMap.acquireLock(index.uid().bytes())
        ) {
            ensureOpen();
            lastWriteNanos = index.startTime();

            // Enforce safe access mode so version map entries are stored.
            // This is required for partial updates to find existing documents via getSourceForPartialUpdate().
            versionMap.enforceSafeAccess();

            boolean isExternalVersioning = index.versionType() == VersionType.EXTERNAL;
            if (index.getAutoGeneratedIdTimestamp() == UNSET_AUTO_GENERATED_TIMESTAMP) {
                validateDocumentVersion(index);
            }

            if (isExternalVersioning) {
                index.parsedDoc().version().setLongValue(index.version());
            }

            IndexResult indexResult = indexIntoLucene(index, isCreateMode);
            if (indexResult.getResultType() == Result.Type.SUCCESS) {
                // Always update version map after successful write, just like InternalEngine.
                // This is critical for real-time gets (including partial updates) to see the latest data.
                // Without this, getSourceForPartialUpdate() won't find the entry in version map
                // and won't call refresh(), potentially reading stale data.
                versionMap.maybePutIndexUnderLock(
                    index.uid().bytes(),
                    new IndexVersionValue(EMPTY_TRANSLOG_LOCATION, index.version(), index.seqNo(), index.primaryTerm())
                );
            }
        } catch (VersionConflictEngineException e) {
            logger.debug("Version conflict encountered when processing index operation", e);
            throw e;
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    private IndexResult indexIntoLucene(Index index, boolean isCreateMode) throws IOException {
        if (isCreateMode || index.getAutoGeneratedIdTimestamp() != UNSET_AUTO_GENERATED_TIMESTAMP) {
            addDocs(index.docs(), documentIndexWriter, index.uid());
        } else {
            updateDocs(index.uid(), index.docs(), documentIndexWriter, index.version(), index.seqNo(), index.primaryTerm());
        }
        return new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
    }

    private void addDocs(final List<ParseContext.Document> docs, final DocumentIndexWriter indexWriter, Term uid) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs, uid);
        } else {
            indexWriter.addDocument(docs.get(0), uid);
        }
    }

    private void updateDocs(
        final Term uid,
        final List<ParseContext.Document> docs,
        final DocumentIndexWriter indexWriter,
        long version,
        long seqNo,
        long primaryTerm
    ) throws IOException {
        if (docs.size() > 1) {
            indexWriter.softUpdateDocuments(uid, docs, version, seqNo, primaryTerm, softDeletesField);
        } else {
            indexWriter.softUpdateDocument(uid, docs.get(0), version, seqNo, primaryTerm, softDeletesField);
        }
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        throw new IngestionEngineException("push-based deletion is not supported in ingestion engine, use streaming source instead");
    }

    /**
     * Processes delete operations. This is used internally by the stream poller only.
     */
    public void deleteInternal(Delete delete) throws IOException {
        versionMap.enforceSafeAccess();
        assert Objects.equals(delete.uid().field(), IdFieldMapper.NAME) : delete.uid().field();
        lastWriteNanos = delete.startTime();

        try (
            ReleasableLock releasableLock1 = readLock.acquire();
            Releasable releasableLock2 = versionMap.acquireLock(delete.uid().bytes())
        ) {
            ensureOpen();
            validateDocumentVersion(delete);
            final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newDeleteTombstoneDoc(delete.id());
            boolean isExternalVersioning = delete.versionType() == VersionType.EXTERNAL;
            if (isExternalVersioning) {
                tombstone.version().setLongValue(delete.version());
            }

            assert tombstone.docs().size() == 1 : "Tombstone doc should have single doc [" + tombstone + "]";
            final ParseContext.Document doc = tombstone.docs().get(0);
            assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null : "Delete tombstone document but _tombstone field is not set ["
                + doc
                + " ]";
            doc.add(softDeletesField);
            documentIndexWriter.deleteDocument(
                delete.uid(),
                false,
                doc,
                delete.version(),
                delete.seqNo(),
                delete.primaryTerm(),
                softDeletesField
            );
            // Always update version map after delete, just like InternalEngine.
            // This is critical for real-time gets to correctly identify deleted documents.
            versionMap.putDeleteUnderLock(
                delete.uid().bytes(),
                new DeleteVersionValue(
                    delete.version(),
                    delete.seqNo(),
                    delete.primaryTerm(),
                    engineConfig.getThreadPool().relativeTimeInMillis()
                )
            );
        } catch (VersionConflictEngineException e) {
            logger.debug("Version conflict encountered when processing deletes", e);
            throw e;
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("delete", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }

        maybePruneDeletes();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        ensureOpen();
        NoOpResult noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        return noOpResult;
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
    }

    /**
     * Gets the document source for partial update operations. This method retrieves the existing document
     * from the index so it can be merged with the partial update.
     *
     * <p>This method performs a real-time get by:
     * 1. Checking the version map for the latest version info
     * 2. If found, checking if refresh is needed based on pointer comparison
     * 3. If current minSuccessfulPointer > lastRefreshedSuccessfulPointer, refresh is needed
     * 4. Reading the document from Lucene
     *
     * <p>This ensures we always read the most recent version of the document, similar to how
     * the push-based flow handles updates via {@link org.opensearch.index.get.ShardGetService#getForUpdate}.
     *
     * <p>The optimization here is that we avoid unnecessary refreshes by tracking:
     * - lastRefreshedSuccessfulPointer: the minimum successful pointer at the last refresh
     * - current minSuccessfulPointer: the minimum of successfully processed pointers across all threads
     * If these are the same, no new writes have completed since last refresh, so we can skip refresh.
     *
     * @param id the document ID
     * @return BytesReference containing the document source, or null if document doesn't exist
     */
    @Nullable
    public BytesReference getSourceForPartialUpdate(String id) {
        ensureOpen();
        Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(id));

        // Check version map for the latest version info (similar to realtime get in InternalEngine)
        // We need to lock to safely access the version map
        try (Releasable ignore = versionMap.acquireLock(uidTerm.bytes())) {
            VersionValue versionValue = versionMap.getUnderLock(uidTerm.bytes());
            if (versionValue != null) {
                if (versionValue.isDelete()) {
                    // Document was deleted - return null to treat as upsert
                    return null;
                }
                // Check if refresh is needed based on pointer comparison
                // Only refresh if new writes have completed since the last refresh
                // Important: capture the pointer BEFORE refresh to avoid race condition
                IngestionShardPointer pointerBeforeRefresh = getCurrentMinSuccessfulPointer();
                if (needsRefreshForPartialUpdate(pointerBeforeRefresh)) {
                    refresh("partial_update_get", SearcherScope.INTERNAL, true);
                    // Update with the pointer we captured before refresh, not the current one
                    updateLastRefreshedSuccessfulPointer(pointerBeforeRefresh);
                }
            }
        }

        // Now read from Lucene - the internal reader is guaranteed to have the latest data
        Get get = new Get(false, false, id, uidTerm);
        try (GetResult getResult = getFromSearcher(get, this::acquireSearcher, SearcherScope.INTERNAL)) {
            if (!getResult.exists()) {
                return null;
            }

            // Load the source from stored fields
            VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion = getResult.docIdAndVersion();
            FieldsVisitor fieldsVisitor = new FieldsVisitor(true);
            try {
                docIdAndVersion.reader.storedFields().document(docIdAndVersion.docId, fieldsVisitor);
                return fieldsVisitor.source();
            } catch (IOException e) {
                throw new EngineException(shardId, "Failed to load source for document [" + id + "]", e);
            }
        }
    }

    /**
     * Gets the current minimum successful pointer from the stream poller.
     *
     * @return the current minimum successful pointer, or null if not available
     */
    @Nullable
    private IngestionShardPointer getCurrentMinSuccessfulPointer() {
        if (streamPoller == null) {
            return null;
        }
        return streamPoller.getMinSuccessfulPointer();
    }

    /**
     * Checks if a refresh is needed before reading for partial update.
     * Refresh is needed if there are new successful writes since the last refresh.
     * This is determined by comparing the provided pointer with the pointer recorded
     * at the last refresh.
     *
     * @param currentMinPointer the current minimum successful pointer (captured before refresh)
     * @return true if refresh is needed, false otherwise
     */
    private boolean needsRefreshForPartialUpdate(@Nullable IngestionShardPointer currentMinPointer) {
        if (streamPoller == null) {
            // Poller not initialized yet, be conservative and refresh
            return true;
        }

        if (currentMinPointer == null) {
            // No successful writes yet, no refresh needed
            return false;
        }

        if (lastRefreshedSuccessfulPointer == null) {
            // First partial update, need to refresh
            return true;
        }

        // Refresh needed if current pointer is ahead of last refreshed pointer
        return currentMinPointer.compareTo(lastRefreshedSuccessfulPointer) > 0;
    }

    /**
     * Updates the lastRefreshedSuccessfulPointer to the provided pointer.
     * Called after a refresh is performed for partial update.
     *
     * @param pointer the pointer to set (should be the pointer captured before refresh)
     */
    private void updateLastRefreshedSuccessfulPointer(@Nullable IngestionShardPointer pointer) {
        if (pointer != null) {
            lastRefreshedSuccessfulPointer = pointer;
        }
    }

    @Override
    protected void pruneDeletedTombstones() {
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        // prune based only on timestamp and not sequence number
        versionMap.pruneTombstones(maxTimestampToPrune, Long.MAX_VALUE);
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return EMPTY_TRANSLOG_SNAPSHOT;
    }

    /**
     * This method is a copy of commitIndexWriter method from {@link InternalEngine} with some additions for ingestion
     * source.
     */
    @Override
    protected void commitIndexWriter(final DocumentIndexWriter writer, final String translogUUID) throws IOException {
        try {
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            final IngestionShardPointer batchStartPointer = streamPoller.getBatchStartPointer();
            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(7);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));

                /*
                 * Ingestion engine needs to record batch start pointer.
                 * Batch start pointer can be null at index creation time, if flush is called before the stream
                 * poller has been completely initialized.
                 */
                if (batchStartPointer != null) {
                    commitData.put(StreamPoller.BATCH_START, batchStartPointer.asString());
                } else {
                    logger.warn("ignore null batch start pointer");
                }
                final String currentForceMergeUUID = forceMergeUUID;
                if (currentForceMergeUUID != null) {
                    commitData.put(FORCE_MERGE_UUID_KEY, currentForceMergeUUID);
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            shouldPeriodicallyFlushAfterBigMerge.set(false);
            writer.commit();
            lastCommittedBatchStartPointer = batchStartPointer;
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            /*
             * If assertions are enabled, IndexWriter throws AssertionError on commit if any files don't exist, but tests that randomly
             * throw FileNotFoundException or NoSuchFileException can also hit this.
             */
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    /**
     * Periodic flush is required if the batchStartPointer has changed since the last commit or there is a big merge.
     */
    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();

        // Check if flush needed after big merge
        if (shouldPeriodicallyFlushAfterBigMerge.get()) {
            return true;
        }

        // Check if batchStartPointer has changed since last commit
        IngestionShardPointer currentBatchStartPointer = streamPoller.getBatchStartPointer();

        // If current pointer is null, no flush needed
        if (currentBatchStartPointer == null) {
            return false;
        }

        // If this is the first commit or pointer has changed, flush is needed
        if (lastCommittedBatchStartPointer == null) {
            return true;
        }

        return currentBatchStartPointer.equals(lastCommittedBatchStartPointer) == false;
    }

    @Override
    public void activateThrottling() {
        // TODO: add this when we have a thread pool for indexing in parallel
    }

    @Override
    public void deactivateThrottling() {
        // TODO: is this needed?
    }

    @Override
    public void close() throws IOException {
        if (streamPoller != null) {
            streamPoller.close();
        }
        unregisterStreamPollerListener();
        super.close();
    }

    public DocumentMapperForType getDocumentMapperForType() {
        return documentMapperForType;
    }

    @Override
    protected TranslogManager createTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        CompositeTranslogEventListener translogEventListener
    ) throws IOException {
        return new NoOpTranslogManager(
            shardId,
            readLock,
            this::ensureOpen,
            new TranslogStats(),
            EMPTY_TRANSLOG_SNAPSHOT,
            translogUUID,
            true
        );
    }

    protected Map<String, String> commitDataAsMap() {
        return commitDataAsMap(documentIndexWriter);
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return streamPoller.getStats();
    }

    private void registerDynamicIndexSettingsHandlers() {
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(IndexMetadata.INGESTION_SOURCE_ERROR_STRATEGY_SETTING, this::updateErrorHandlingStrategy);
    }

    /**
     * Handler for updating ingestion error strategy in the stream poller on dynamic index settings update.
     */
    private void updateErrorHandlingStrategy(IngestionErrorStrategy.ErrorStrategy errorStrategy) {
        IngestionErrorStrategy updatedIngestionErrorStrategy = IngestionErrorStrategy.create(
            errorStrategy,
            engineConfig.getIndexSettings().getIndexMetadata().getIngestionSource().getType()
        );
        streamPoller.updateErrorStrategy(updatedIngestionErrorStrategy);
    }

    /**
     * Handler for updating ingestion source params on dynamic index settings update.
     * This will reinitialize the streamPoller's consumer with new configurations.
     */
    private void updateIngestionSourceParams(Map<String, Object> updatedParams) {
        if (streamPoller.getConsumer() == null) {
            logger.debug("Consumer not yet initialized, skipping consumer reinitialization for ingestion source params update");
            return;
        }

        logger.info("Ingestion source params updated, reinitializing consumer");

        // Get current ingestion source with updated params from index metadata
        IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
        assert indexMetadata != null;
        IngestionSource updatedIngestionSource = Objects.requireNonNull(indexMetadata.getIngestionSource());

        // Request consumer reinitialization in the poller
        streamPoller.requestConsumerReinitialization(updatedIngestionSource);
        logger.info("Successfully processed ingestion source params update");
    }

    /**
     * Validates document version for pull-based ingestion. Only external versioning is supported.
     */
    private void validateDocumentVersion(final Operation operation) throws IOException {
        if (operation.versionType() != VersionType.EXTERNAL) {
            return;
        }

        versionMap.enforceSafeAccess();
        final VersionValue versionValue = resolveDocVersion(operation, false);
        final long currentVersion;
        final boolean currentNotFoundOrDeleted;

        if (versionValue == null) {
            // todo: possible to optimize addDoc instead of updateDoc if version is not present?
            currentVersion = Versions.NOT_FOUND;
            currentNotFoundOrDeleted = true;
        } else {
            currentVersion = versionValue.version;
            currentNotFoundOrDeleted = versionValue.isDelete();
        }

        if (operation.versionType().isVersionConflictForWrites(currentVersion, operation.version(), currentNotFoundOrDeleted)) {
            throw new VersionConflictEngineException(shardId, operation, currentVersion, currentNotFoundOrDeleted);
        }
    }

    /**
     * Apply updated ingestion settings, resetting consumer and updating poller.
     */
    public void updateIngestionSettings(IngestionSettings ingestionSettings) {
        // reset poller position and reinitialize poller
        if (ingestionSettings.getResetState() != null && ingestionSettings.getResetValue() != null) {
            resetStreamPoller(ingestionSettings.getResetState(), ingestionSettings.getResetValue());
        }

        // update ingestion state
        if (ingestionSettings.getIsPaused() != null) {
            updateIngestionState(ingestionSettings);
        }
    }

    /**
     * Update ingestion state of the poller.
     */
    private void updateIngestionState(IngestionSettings ingestionSettings) {
        if (ingestionSettings.getIsPaused()) {
            streamPoller.pause();
        } else {
            streamPoller.resume();
        }
    }

    /**
     * Reinitialize the poller with provided state and value. The current poller is first closed, before initializing
     * the new poller. Once new poller is initialized, a flush is triggered to persist the new batch start pointer.
     */
    private void resetStreamPoller(StreamPoller.ResetState resetState, String resetValue) {
        if (streamPoller.isPaused() == false) {
            throw new IllegalStateException("Cannot reset consumer when poller is not paused");
        }

        if (streamPoller.getConsumer() == null) {
            throw new IllegalStateException("Consumer is not yet initialized");
        }

        try {
            // refresh is needed for persisted pointers to be visible
            refresh("reset poller", SearcherScope.INTERNAL, true);

            IngestionShardPointer startPointer = null;
            if (resetState == StreamPoller.ResetState.RESET_BY_OFFSET) {
                startPointer = streamPoller.getConsumer().pointerFromOffset(resetValue);
            } else if (resetState == StreamPoller.ResetState.RESET_BY_TIMESTAMP) {
                startPointer = streamPoller.getConsumer().pointerFromTimestampMillis(Long.parseLong(resetValue));
            }

            streamPoller.close();
            unregisterStreamPollerListener();
            initializeStreamPoller(resetState, resetValue, startPointer);
        } catch (Exception e) {
            throw new OpenSearchException("Failed to reset stream poller", e);
        }

        try {
            // force flush to persist the new batch start pointer
            flush(true, true);
        } catch (Exception e) {
            throw new OpenSearchException("Exception during flush. Poller successfully reset, but reset value might not be persisted.", e);
        }
    }

    /**
     * Get current ingestion state. Used by management flows.
     */
    public ShardIngestionState getIngestionState() {
        IngestionShardPointer shardPointer = streamPoller.getBatchStartPointer();

        return new ShardIngestionState(
            engineConfig.getIndexSettings().getIndex().getName(),
            engineConfig.getShardId().getId(),
            streamPoller.getState().toString(),
            streamPoller.getErrorStrategy().getName(),
            streamPoller.isPaused(),
            streamPoller.isWriteBlockEnabled(),
            shardPointer != null ? shardPointer.toString() : ""
        );
    }
}
