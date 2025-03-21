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
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.streamingingestion.state.TransportUpdateIngestionStateAction;
import org.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;

/**
 * Service responsible for submitting metadata updates (for example, ingestion pause/resume state change updates).
 *
 * @opensearch.experimental
 */
public class MetadataStreamingIngestionStateService {
    private static final Logger logger = LogManager.getLogger(MetadataStreamingIngestionStateService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportUpdateIngestionStateAction transportUpdateIngestionStateAction;
    private final IndicesService indicesService;

    @Inject
    public MetadataStreamingIngestionStateService(
        ClusterService clusterService,
        TransportUpdateIngestionStateAction transportUpdateIngestionStateAction,
        IndicesService indicesService,
        ThreadPool threadPool
        ) {
        this.clusterService = clusterService;
        this.transportUpdateIngestionStateAction = transportUpdateIngestionStateAction;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
    }

    /**
     * Publishes cluster state change request to pause ingestion.
     */
//    public void pauseIngestion(
//        final PauseIngestionClusterStateUpdateRequest request,
//        final ActionListener<PauseIngestionResponse> listener
//    ) {
//        final Index[] indices = request.indices();
//        if (indices == null || indices.length == 0) {
//            throw new IllegalArgumentException("Index is required");
//        }
//
//        clusterService.submitStateUpdateTask("pause-ingestion", new ClusterStateUpdateTask(Priority.URGENT) {
//
//            @Override
//            public ClusterState execute(ClusterState currentState) {
//                return updateIngestionPausedState(indices, currentState, true);
//            }
//
//            @Override
//            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
//                if (oldState == newState) {
//                    logger.debug("Pause cluster state update processed, but there is no cluster state change.");
//                    listener.onResponse(new PauseIngestionResponse(true, false, new ArrayList<>()));
//                } else {
//                    // todo: should we run this on a different thread?
//                    PauseIngestionResponse response = new PauseIngestionResponse(true, false, new ArrayList<>());
//                    processPauseIngestionOnShards(request, response, listener);
//                }
//            }
//
//            @Override
//            public void onFailure(String source, Exception e) {
//                listener.onFailure(new OpenSearchException("pause ingestion failed", e));
//            }
//
//            @Override
//            public TimeValue timeout() {
//                return request.clusterManagerNodeTimeout();
//            }
//        });
//    }

    /**
     * Publishes cluster state change request to resume ingestion.
     */
//    public void resumeIngestion(
//        final ResumeIngestionClusterStateUpdateRequest request,
//        final ActionListener<ResumeIngestionResponse> listener
//    ) {
//        final Index[] indices = request.indices();
//        if (indices == null || indices.length == 0) {
//            throw new IllegalArgumentException("Index name is required");
//        }
//
//        clusterService.submitStateUpdateTask("resume-ingestion", new ClusterStateUpdateTask(Priority.URGENT) {
//
//            @Override
//            public ClusterState execute(ClusterState currentState) {
//                return updateIngestionPausedState(indices, currentState, false);
//            }
//
//            @Override
//            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
//                if (oldState == newState) {
//                    logger.debug("Resume cluster state update processed, but there is no cluster state change.");
//                    listener.onResponse(new ResumeIngestionResponse(true, false, new ArrayList<>()));
//                } else {
//                    // todo: should we run this on a different thread?
//                    ResumeIngestionResponse response = new ResumeIngestionResponse(true, false, new ArrayList<>());
//                    processResumeIngestionOnShards(request, response, listener);
//                }
//            }
//
//            @Override
//            public void onFailure(String source, Exception e) {
//                listener.onFailure(new OpenSearchException("resume ingestion failed", e));
//            }
//
//            @Override
//            public TimeValue timeout() {
//                return request.clusterManagerNodeTimeout();
//            }
//        });
//    }

    public void updateIngestionPollerState(String source, Index[] concreteIndices, UpdateIngestionStateRequest request, ActionListener<UpdateIngestionStateResponse> listener) {
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index  is missing");
        }

        if (request.getIngestionPaused() == null) {
            throw new IllegalArgumentException("Ingestion poller target state is missing");
        }

        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                return getUpdatedIngestionPausedClusterState(concreteIndices, currentState, request.getIngestionPaused());
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                if (oldState == newState) {
                    logger.debug("Cluster state did not change when trying to set ingestionPaused={}", request.getIngestionPaused());
                    listener.onResponse(new UpdateIngestionStateResponse(false, 0, 0, 0, Collections.emptyList()));
                } else {
                    // todo: should we run this on a different thread?
                    processUpdateIngestionRequestOnShards(request,  new ActionListener<>() {

                        @Override
                        public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                            listener.onResponse(updateIngestionStateResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            UpdateIngestionStateResponse response = new UpdateIngestionStateResponse(true, 0, 0, 0, Collections.emptyList());
                            response.setErrorMessage("Error encountered while verifying ingestion poller state: " + e.getMessage());
                            listener.onResponse(response);
                        }
                    });
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(new OpenSearchException("Ingestion cluster state update failed to set ingestionPaused={}", request.getIngestionPaused(), e));
            }

            @Override
            public TimeValue timeout() {
                return request.timeout();
            }
        });
    }

    public void processUpdateIngestionRequestOnShards(UpdateIngestionStateRequest updateIngestionStateRequest, ActionListener<UpdateIngestionStateResponse> listener) {
        transportUpdateIngestionStateAction.execute(updateIngestionStateRequest, listener);
    }

    private ClusterState getUpdatedIngestionPausedClusterState(final Index[] indices, final ClusterState currentState, boolean ingestionPaused) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);

            if (indexMetadata.useIngestionSource() == false) {
                logger.debug("Pause/resume request will be ignored for index {} as streaming ingestion is not enabled", index);
            }

            if (indexMetadata.isIngestionPaused() != ingestionPaused) {
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).ingestionPaused(ingestionPaused);
                metadata.put(updatedMetadata);
            } else {
                logger.debug(
                    "Received request for ingestionPaused:{} for index {}. The state is already ingestionPaused:{}",
                    ingestionPaused,
                    index,
                    ingestionPaused
                );
            }
        }

        return ClusterState.builder(currentState).metadata(metadata).build();
    }

//    private void processPauseIngestionOnShards2(PauseIngestionClusterStateUpdateRequest request, PauseIngestionResponse response, ActionListener<PauseIngestionResponse> listener) {
//        String[] indexNames = Arrays.stream(request.indices())
//            .map(Index::getName)
//            .toArray(String[]::new);
//        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indexNames, new int[]{});
//        updateIngestionStateRequest.setIngestionPaused(true);
//
//        transportUpdateIngestionStateAction.execute(updateIngestionStateRequest, new ActionListener<>() {
//
//            @Override
//            public void onResponse(GetIngestionStateResponse getIngestionStateResponse) {
//                List<PauseIngestionResponse.ShardFailure> shardFailureList = new ArrayList<>();
//
//                for (ShardIngestionState ingestionState : getIngestionStateResponse.getShardStates()) {
//                    if (ingestionState.isPollerPaused() == false) {
//                        shardFailureList.add(new PauseIngestionResponse.ShardFailure(ingestionState.getIndex(), ingestionState.getShardId(), "Shard not yet paused"));
//                    }
//                }
//
//                if (getIngestionStateResponse.getShardFailures() != null) {
//                    for (DefaultShardOperationFailedException failedException : getIngestionStateResponse.getShardFailures()) {
//                        shardFailureList.add(new PauseIngestionResponse.ShardFailure(failedException.index(), failedException.shardId(), failedException.reason()));
//                    }
//                }
//
//                boolean shardsAcked = shardFailureList.isEmpty();
//                response.setShardsAcknowledged(shardsAcked);
//                response.setShardFailuresList(shardFailureList);
//                listener.onResponse(response);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                response.setError("Error validating index shards states: " + e.getMessage());
//                listener.onResponse(response);
//            }
//        });
//    }

//    private void processResumeIngestionOnShards(ResumeIngestionClusterStateUpdateRequest request, ResumeIngestionResponse response, ActionListener<ResumeIngestionResponse> listener) {
//        String[] indexNames = Arrays.stream(request.indices())
//            .map(Index::getName)
//            .toArray(String[]::new);
//        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indexNames, new int[]{});
//        updateIngestionStateRequest.setIngestionPaused(false);
//
//        transportUpdateIngestionStateAction.execute(updateIngestionStateRequest, new ActionListener<>() {
//
//            @Override
//            public void onResponse(GetIngestionStateResponse getIngestionStateResponse) {
//                List<ResumeIngestionResponse.ShardFailure> shardFailureList = new ArrayList<>();
//
//                for (ShardIngestionState ingestionState : getIngestionStateResponse.getShardStates()) {
//                    if (ingestionState.isPollerPaused()) {
//                        shardFailureList.add(new ResumeIngestionResponse.ShardFailure(ingestionState.getIndex(), ingestionState.getShardId(), "Shard not yet resumed"));
//                    }
//                }
//
//                if (getIngestionStateResponse.getShardFailures() != null) {
//                    for (DefaultShardOperationFailedException failedException : getIngestionStateResponse.getShardFailures()) {
//                        shardFailureList.add(new ResumeIngestionResponse.ShardFailure(failedException.index(), failedException.shardId(), failedException.reason()));
//                    }
//                }
//
//                boolean shardsAcked = shardFailureList.isEmpty();
//                response.setShardsAcknowledged(shardsAcked);
//                response.setShardFailuresList(shardFailureList);
//                listener.onResponse(response);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                response.setError("Error validating index shards states: " + e.getMessage());
//                listener.onResponse(response);
//            }
//        });
//    }
}
