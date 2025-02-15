/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.translog.WriteOnlyTranslogManager;

import java.io.IOException;
import java.util.Collection;

import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_LOCATION;
import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_SNAPSHOT;

/**
 * This is a {@link NRTReplicationEngine} variant to be used with pull-based ingestion on replica shards when segment
 * replication is enabled. This engine uses a no-op translog.
 */
public class IngestionNRTReplicationEngine extends NRTReplicationEngine {
    protected final TranslogManager translogManager;

    public IngestionNRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);

        try {
            this.translogManager = new NoOpTranslogManager(
                shardId,
                readLock,
                this::ensureOpen,
                new TranslogStats(0, 0, 0, 0, 0),
                EMPTY_TRANSLOG_SNAPSHOT
            );
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        }
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    protected WriteOnlyTranslogManager createWriteOnlyTranslogManager() {
        // This engine does not use a translog. A separate NoOpTranslogManager will be used.
        return null;
    }

    @Override
    public synchronized void updateSegments(final SegmentInfos infos) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            // Update the current infos reference on the Engine's reader.
            ensureOpen();
            final long incomingGeneration = infos.getGeneration();
            readerManager.updateSegments(infos);
            if (incomingGeneration != this.lastReceivedPrimaryGen) {
                flush(false, true);
            }
            this.lastReceivedPrimaryGen = incomingGeneration;
        }
    }

    /**
     * Persist the latest live SegmentInfos. This method creates a commit point from the latest SegmentInfos.
     *
     * @throws IOException - When there is an IO error committing the SegmentInfos.
     */
    @Override
    protected void commitSegmentInfos(SegmentInfos infos) throws IOException {
        // get a reference to the previous commit files so they can be decref'd once a new commit is made.
        final Collection<String> previousCommitFiles = getLastCommittedSegmentInfos().files(true);
        store.commitSegmentInfos(infos, 0, 0);
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        // incref the latest on-disk commit.
        replicaFileTracker.incRef(this.lastCommittedSegmentInfos.files(true));
        // decref the prev commit.
        replicaFileTracker.decRef(previousCommitFiles);
    }

    @Override
    protected Translog.Location getTranslogIndexLocation(Index index, IndexResult indexResult) throws IOException {
        return EMPTY_TRANSLOG_LOCATION;
    }

    @Override
    protected Translog.Location getTranslogDeleteLocation(Delete delete, DeleteResult deleteResult) throws IOException {
        return EMPTY_TRANSLOG_LOCATION;
    }

    @Override
    protected Translog.Location getTranslogNoOpLocation(NoOp noOp) throws IOException {
        return EMPTY_TRANSLOG_LOCATION;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        // sequence numbers are not used as ingestion only supports segment replication
        return new SeqNoStats(0, 0, 0);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {}

}
