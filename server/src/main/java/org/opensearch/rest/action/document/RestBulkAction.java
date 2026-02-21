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

package org.opensearch.rest.action.document;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.transport.client.Requests;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 *
 * @opensearch.api
 */
public class RestBulkAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestBulkAction.class);

    private final boolean allowExplicitIndex;

    public RestBulkAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    /**
     * Validates bulk payload structure. Returns true if corruption is detected.
     * A valid bulk request alternates: action line, document line, action line, document line...
     * Document lines should NOT start with action metadata like {"create", {"index", {"delete", {"update"
     */
    private static boolean detectBulkPayloadCorruption(BytesReference content) {
        try {
            String body = content.utf8ToString();
            String[] lines = body.split("\n");
            
            // Check document lines (odd indices: 1, 3, 5, ...) for action metadata
            // These should be document content, not action lines
            for (int i = 1; i < lines.length; i += 2) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;
                
                // Document content should not start with bulk action metadata
                if (line.startsWith("{\"create\"") || 
                    line.startsWith("{\"index\"") || 
                    line.startsWith("{\"delete\"") || 
                    line.startsWith("{\"update\"")) {
                    logger.error(
                        "BULK_PAYLOAD_CORRUPTION detected at REST layer! " +
                        "Document line {} starts with action metadata. Line content: [{}], " +
                        "Full payload (base64): {}",
                        i, 
                        line.substring(0, Math.min(200, line.length())),
                        Base64.getEncoder().encodeToString(BytesReference.toBytes(content))
                    );
                    return true;
                }
            }
        } catch (Exception e) {
            // If we can't parse, log but don't block
            logger.warn("Failed to validate bulk payload: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(POST, "/_bulk"), new Route(PUT, "/_bulk"), new Route(POST, "/{index}/_bulk"), new Route(PUT, "/{index}/_bulk"))
        );
    }

    @Override
    public String getName() {
        return "bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        BulkRequest bulkRequest = Requests.bulkRequest();
        String defaultIndex = request.param("index");
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String defaultPipeline = request.param("pipeline");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, null);
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        
        BytesReference content = request.requiredContent();
        
        // DEBUG: Check for corruption at REST entry point
        detectBulkPayloadCorruption(content);
        
        bulkRequest.add(
            content,
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            allowExplicitIndex,
            request.getMediaType()
        );

        return channel -> client.bulk(bulkRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}
