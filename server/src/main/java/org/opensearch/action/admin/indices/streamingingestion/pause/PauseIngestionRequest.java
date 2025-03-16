/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to pause pull-based ingestion.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class PauseIngestionRequest extends AcknowledgedRequest<PauseIngestionRequest> implements IndicesRequest.Replaceable {

    private boolean verbose = false;
    private String[] indices;
    private final List<Integer> shardIds;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public PauseIngestionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.verbose = in.readBoolean();
        int[] shardIdsArray = in.readIntArray();
        this.shardIds = Arrays.stream(shardIdsArray).boxed().toList();
    }

    /**
     * Constructs a new pause ingestion request for specified index and shards.
     */
    public PauseIngestionRequest(String[] indices) {
        this(indices, Collections.emptyList(), false);
    }

    /**
     * Constructs a new pause ingestion request for specified index and shards.
     */
    public PauseIngestionRequest(String[] indices, List<Integer> shardIds, boolean verbose) {
        this.indices = indices;
        this.shardIds = shardIds;
        this.verbose = verbose;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * The indices to be closed
     * @return the indices to be closed
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be closed
     * @param indices the indices to be closed
     * @return the request itself
     */
    @Override
    public PauseIngestionRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public PauseIngestionRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeBoolean(verbose);
        int[] shardIdsArray = shardIds.stream().mapToInt(Integer::intValue).toArray();
        out.writeIntArray(shardIdsArray);
    }

    /**
     * <code>true</code> if detailed information about each segment should be returned,
     * <code>false</code> otherwise.
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets the <code>verbose</code> option.
     * @see #verbose()
     */
    public void verbose(boolean v) {
        verbose = v;
    }
}
