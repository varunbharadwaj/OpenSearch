/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class IngestionNRTReplicationEngineTests extends EngineTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build()
    );

    public void testCreateEngine() throws IOException {
        try (
            final Store store = createStore(INDEX_SETTINGS, newDirectory());
            final IngestionNRTReplicationEngine nrtEngine = buildIngestionNrtReplicaEngine(store, INDEX_SETTINGS)
        ) {
            final SegmentInfos latestSegmentInfos = nrtEngine.getLatestSegmentInfos();
            final SegmentInfos lastCommittedSegmentInfos = nrtEngine.getLastCommittedSegmentInfos();
            assertEquals(latestSegmentInfos.version, lastCommittedSegmentInfos.version);
            assertEquals(latestSegmentInfos.getGeneration(), lastCommittedSegmentInfos.getGeneration());
            assertEquals(latestSegmentInfos.getUserData(), lastCommittedSegmentInfos.getUserData());
            assertEquals(latestSegmentInfos.files(true), lastCommittedSegmentInfos.files(true));
            assertTrue(nrtEngine.segments(true).isEmpty());

            try (final GatedCloseable<IndexCommit> indexCommitGatedCloseable = nrtEngine.acquireLastIndexCommit(false)) {
                final IndexCommit indexCommit = indexCommitGatedCloseable.get();
                assertEquals(indexCommit.getUserData(), lastCommittedSegmentInfos.getUserData());
                assertTrue(indexCommit.getFileNames().containsAll(lastCommittedSegmentInfos.files(true)));
            }
        }
    }

    public void testCommitSegment() throws Exception {
        try (
            final Store store = createStore(INDEX_SETTINGS, newDirectory());
            final IngestionNRTReplicationEngine nrtEngine = buildIngestionNrtReplicaEngine(store, INDEX_SETTINGS)
        ) {
            List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean(), randomBoolean())
                .stream()
                .filter(op -> op.operationType().equals(Engine.Operation.TYPE.INDEX))
                .collect(Collectors.toList());
            for (Engine.Operation op : operations) {
                applyOperation(nrtEngine, op);
            }

            final String lastCommitSegmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(store.directory());
            final SegmentInfos committedInfos = SegmentInfos.readCommit(store.directory(), lastCommitSegmentsFileName);
            try (final GatedCloseable<IndexCommit> indexCommit = nrtEngine.acquireLastIndexCommit(true)) {
                assertEquals(committedInfos.getGeneration() + 1, indexCommit.get().getGeneration());
            }
        }
    }

    public void testUpdateSegments() throws Exception {
        try (
            final Store store = createStore(INDEX_SETTINGS, newDirectory());
            final IngestionNRTReplicationEngine nrtEngine = buildIngestionNrtReplicaEngine(store, INDEX_SETTINGS)
        ) {
            nrtEngine.flush(true, true);
            assertEquals(2, nrtEngine.getLatestSegmentInfos().getGeneration());
            assertEquals(nrtEngine.getLatestSegmentInfos().getGeneration(), nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(engine.getLatestSegmentInfos().getGeneration(), nrtEngine.getLatestSegmentInfos().getGeneration());

            engine.flush(true, true);
            assertEquals(3, engine.getLatestSegmentInfos().getGeneration());

            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            assertEquals(3, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(3, nrtEngine.getLatestSegmentInfos().getGeneration());
        }
    }

    private IngestionNRTReplicationEngine buildIngestionNrtReplicaEngine(Store store, IndexSettings settings) throws IOException {
        Lucene.cleanLuceneIndex(store.directory());
        final EngineConfig replicaConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, () -> 0);
        if (Lucene.indexExists(store.directory()) == false) {
            store.createEmpty(replicaConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
        }
        return new IngestionNRTReplicationEngine(replicaConfig);
    }

}
