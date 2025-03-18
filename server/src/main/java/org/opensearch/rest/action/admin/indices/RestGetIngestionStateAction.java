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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to pause pull-based ingestion.
 *
 * @opensearch.api
 */
public class RestGetIngestionStateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetIngestionStateAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/{index}/ingestion/_state")));
    }

    @Override
    public String getName() {
        return "get_ingestion_state_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetIngestionStateRequest getIngestionStateRequest = new GetIngestionStateRequest(request.param("index"));

        if (request.hasParam("shards")) {
            int[] shards = Arrays.stream(request.paramAsStringArrayOrEmptyIfAll("shards"))
                .mapToInt(Integer::parseInt) // Convert each string to int
                .toArray();
            getIngestionStateRequest.setShards(shards);
        }
        getIngestionStateRequest.timeout(request.paramAsTime("timeout", getIngestionStateRequest.timeout()));
        getIngestionStateRequest.indicesOptions(IndicesOptions.fromRequest(request, getIngestionStateRequest.indicesOptions()));

        return channel -> client.admin().indices().getIngestionState(getIngestionStateRequest, new RestToXContentListener<>(channel));
    }

}
