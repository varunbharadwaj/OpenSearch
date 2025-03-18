/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionAction;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataStreamingIngestionStateService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * Close index action
 *
 * @opensearch.internal
 */
public class TransportResumeIngestionAction extends TransportClusterManagerNodeAction<ResumeIngestionRequest, ResumeIngestionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportResumeIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportResumeIngestionAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ClusterSettings clusterSettings,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            ResumeIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ResumeIngestionRequest::new,
            indexNameExpressionResolver
        );
        this.ingestionStateService = ingestionStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ResumeIngestionResponse read(StreamInput in) throws IOException {
        return new ResumeIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, ResumeIngestionRequest request, ActionListener<ResumeIngestionResponse> listener) {
//        destructiveOperations.failDestructive(request.indices());
//        if (closeIndexEnabled == false) {
//            throw new IllegalStateException(
//                "closing indices is disabled - set ["
//                    + CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey()
//                    + ": true] to enable it. NOTE: closed indices still consume a significant amount of diskspace"
//            );
//        }
//        super.doExecute(task, request, listener);
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(ResumeIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new ResumeIngestionResponse(true, Collections.emptyList()));
            return;
        }

        final ResumeIngestionClusterStateUpdateRequest resumeRequest = new ResumeIngestionClusterStateUpdateRequest(task.getId()).ackTimeout(
            request.timeout()
        )
            .resetSettings(request.getResetSettingsList())
            .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
            .indices(concreteIndices);

        ingestionStateService.resumeIngestion(resumeRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to resume indices [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }
}
