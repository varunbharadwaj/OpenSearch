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

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.tiering.HotToWarmTieringResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.ShardsAcknowledgedResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Transport response for pausing pull-based ingestion.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class PauseIngestionResponse extends AcknowledgedResponse {

    private final List<IndexResult> indices;

    PauseIngestionResponse(StreamInput in) throws IOException {
        super(in, true);
        indices = unmodifiableList(in.readList(IndexResult::new));
    }

    public PauseIngestionResponse(final boolean acknowledged, final List<IndexResult> indexResults) {
        super(acknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indexResults));
    }

    public List<IndexResult> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(indices);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startArray("indices");
        for (IndexResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Represents result for an index.
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class IndexResult implements Writeable, ToXContentFragment {
        private final String index;
        private final String errorMessage;

        public IndexResult(String index, String errorMessage) {
            this.index = index;
            this.errorMessage = errorMessage;
        }

        IndexResult(StreamInput in) throws IOException {
            this.index = in.readString();
            this.errorMessage = in.readString();
        }

        public String getIndex() {
            return index;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(errorMessage);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index", index);
            builder.field("error", errorMessage);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PauseIngestionResponse.IndexResult that = (PauseIngestionResponse.IndexResult) o;
            return Objects.equals(index, that.index) && Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(index);
            result = 31 * result + Objects.hashCode(errorMessage);
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }
}
