/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Service responsible for submitting open/close index requests as well as for adding index blocks
 *
 * @opensearch.internal
 */
public class MetadataStreamingIngestionStateService {
    private static final Logger logger = LogManager.getLogger(MetadataStreamingIngestionStateService.class);

    private final ClusterService clusterService;

    @Inject
    public MetadataStreamingIngestionStateService(
        ClusterService clusterService
    ) {
        this.clusterService = clusterService;
    }

    /**
     * Closes one or more indices.
     * <p>
     * Closing indices is a 3 steps process: it first adds a write block to every indices to close, then waits for the operations on shards
     * to be terminated and finally closes the indices by moving their state to CLOSE.
     */
    public void pauseIngestion(final PauseIngestionClusterStateUpdateRequest request, final ActionListener<PauseIngestionResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "pause-ingestion " + Arrays.toString(concreteIndices),
            new ClusterStateUpdateTask(Priority.URGENT) {

                private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return updateIngestionPausedState(concreteIndices, currentState, true);
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    // todo: verify pollers have been paused on all the requested index shards
                    boolean shardsAcked = oldState != newState;
                    listener.onResponse(new PauseIngestionResponse(true, shardsAcked, Collections.emptyList()));
                }

                @Override
                public void onFailure(final String source, final Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public TimeValue timeout() {
                    return request.clusterManagerNodeTimeout();
                }
            }
        );
    }

    public void resumeIngestion(final ResumeIngestionClusterStateUpdateRequest request, final ActionListener<ResumeIngestionResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "resume-ingestion " + Arrays.toString(concreteIndices),
            new ClusterStateUpdateTask(Priority.URGENT) {

                private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return updateIngestionPausedState(concreteIndices, currentState, false);
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    // todo: verify pollers have been paused on all the requested index shards
                    boolean shardsAcked = oldState != newState;
                    listener.onResponse(new ResumeIngestionResponse(true, shardsAcked, Collections.emptyList()));
                }

                @Override
                public void onFailure(final String source, final Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public TimeValue timeout() {
                    return request.clusterManagerNodeTimeout();
                }
            }
        );
    }

    static ClusterState updateIngestionPausedState(final Index[] indices, final ClusterState currentState, boolean ingestionPaused) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);

            if (indexMetadata.useIngestionSource() == false) {
                logger.debug("Pause request will be ignored for index {} as streaming ingestion is not enabled", index);
            }

            if (indexMetadata.isIngestionPaused() != ingestionPaused) {
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).ingestionPaused(ingestionPaused);
                metadata.put(updatedMetadata);
            } else {
                logger.debug("Received request for ingestionPaused:{} for index {}. The state is already ingestionPaused:{}", ingestionPaused, index, ingestionPaused);
            }
        }

        return ClusterState.builder(currentState).metadata(metadata).build();
    }
}
