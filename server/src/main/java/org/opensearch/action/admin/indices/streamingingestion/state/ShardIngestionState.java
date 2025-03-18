/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState.Fields.ERROR_POLICY;
import static org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState.Fields.POLLER_STATE;
import static org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState.Fields.SHARD;

/**
 * Ingestion shard state.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class ShardIngestionState implements Writeable, ToXContentFragment {

    private int shardId;
    @Nullable
    private String pollerState;
    @Nullable
    private String errorPolicy;

    public ShardIngestionState() {
        shardId = -1;
    }

    public ShardIngestionState(StreamInput in) throws IOException {
        shardId = in.readInt();
        pollerState = in.readString();
        errorPolicy = in.readString();
    }

    public ShardIngestionState(int shardId, @Nullable String pollerState, @Nullable String errorPolicy) {
        this.shardId = shardId;
        this.pollerState = pollerState;
        this.errorPolicy = errorPolicy;
    }

    public int getShardId() {
        return shardId;
    }

    public String getPollerState() {
        return pollerState;
    }

    public String getErrorPolicy() {
        return errorPolicy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardId);
        out.writeString(pollerState);
        out.writeString(errorPolicy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shardId);
        builder.field(POLLER_STATE, pollerState);
        builder.field(ERROR_POLICY, errorPolicy);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SHARD = "shard";
        static final String POLLER_STATE = "poller_state";
        static final String ERROR_POLICY = "error_policy";
    }
}
