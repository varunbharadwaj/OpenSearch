/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Holds metadata required for updating ingestion state. This is for internal use only and will not be exposed to the user.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class UpdateIngestionStateRequest extends BroadcastRequest<UpdateIngestionStateRequest> {

    private String[] index;
    private int[] shards;

    @Nullable
    private Boolean ingestionPaused;

    public UpdateIngestionStateRequest(String[] index, int[] shards) {
        super();
        this.index = index;
        this.shards = shards;
    }

    public UpdateIngestionStateRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readStringArray();
        this.shards = in.readIntArray();
        this.ingestionPaused = in.readOptionalBoolean();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(index);
        out.writeIntArray(shards);
        out.writeOptionalBoolean(ingestionPaused);
    }

    public String[] getIndex() {
        return index;
    }

    public int[] getShards() {
        return shards;
    }

    public void setShards(int[] shards) {
        this.shards = shards;
    }

    public Boolean getIngestionPaused() {
        return ingestionPaused;
    }

    public void setIngestionPaused(boolean ingestionPaused) {
        this.ingestionPaused = ingestionPaused;
    }
}
