/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka ingestion.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IngestFromKafkaIT extends KafkaIngestionBaseIT {
    /**
     * test ingestion-kafka-plugin is installed
     */
    public void testPluginsAreInstalled() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(
            pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.plugin.kafka.KafkaPlugin"))
        );
    }

    public void testKafkaIngestion() {
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        createIndexWithDefaultSettings(1, 0);

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            assertNotNull(stats);
            assertThat(stats.getMessageProcessorStats().totalProcessedCount(), is(2L));
            assertThat(stats.getConsumerStats().totalPolledCount(), is(2L));
        });
    }

    public void testKafkaIngestion_RewindByTimeStamp() {
        produceData("1", "name1", "24", 1739459500000L, "index");
        produceData("2", "name2", "20", 1739459800000L, "index");

        // create an index with ingestion source from kafka
        createIndex(
            "test_rewind_by_timestamp",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_timestamp")
                // 1739459500000 is the timestamp of the first message
                // 1739459800000 is the timestamp of the second message
                // by resetting to 1739459600000, only the second message will be ingested
                .put("ingestion_source.pointer.init.reset.value", "1739459600000")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test_rewind_by_timestamp");
            SearchResponse response = client().prepareSearch("test_rewind_by_timestamp").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }

    public void testKafkaIngestion_RewindByOffset() {
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        // create an index with ingestion source from kafka
        createIndex(
            "test_rewind_by_offset",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            refresh("test_rewind_by_offset");
            SearchResponse response = client().prepareSearch("test_rewind_by_offset").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }

    public void testCloseIndex() throws Exception {
        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);
        client().admin().indices().close(Requests.closeIndexRequest(indexName)).get();
    }

    public void testMessageOperationTypes() throws Exception {
        // Step 1: Produce message and wait for it to be searchable

        produceData("1", "name", "25", defaultMessageTimestamp, "index");
        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 25 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        // Step 2: Update age field from 25 to 30 and validate

        produceData("1", "name", "30", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        // Step 3: Delete the document and validate
        produceData("1", "name", "30", defaultMessageTimestamp, "delete");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 0;
        });

        // Step 4: Validate create operation
        produceData("2", "name", "30", defaultMessageTimestamp, "create");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "2"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });
    }

    public void testUpdateWithoutIDField() throws Exception {
        // Step 1: Produce message without ID
        String payload = "{\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 25}}";
        produceData(payload);

        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);

        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("age", "25"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 25 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        SearchResponse searchableDocsResponse = client().prepareSearch(indexName).setSize(10).setPreference("_only_local").get();
        assertThat(searchableDocsResponse.getHits().getTotalHits().value(), is(1L));
        assertEquals(25, searchableDocsResponse.getHits().getHits()[0].getSourceAsMap().get("age"));
        String id = searchableDocsResponse.getHits().getHits()[0].getId();

        // Step 2: Produce an update message using retrieved ID and validate

        produceData(id, "name", "30", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", id));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });
    }

    public void testMultiThreadedWrites() throws Exception {
        // create index with 5 writer threads
        createIndexWithDefaultSettings(indexName, 1, 0, 5);
        ensureGreen(indexName);

        // Step 1: Produce messages
        for (int i = 0; i < 1000; i++) {
            produceData(Integer.toString(i), "name" + i, "25");
        }

        waitForState(() -> {
            SearchResponse searchableDocsResponse = client().prepareSearch(indexName).setSize(2000).setPreference("_only_local").get();
            return searchableDocsResponse.getHits().getTotalHits().value() == 1000;
        });

        // Step 2: Produce an update message and validate
        for (int i = 0; i < 1000; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        waitForState(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(28);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 1000;
        });
    }

    public void testKafkaIngestionWithOffsetRange() throws Exception {
        // Produce 5 messages
        for (int i = 1; i <= 5; i++) {
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i));
        }

        // Create index with offset range: start from offset 1, end at offset 3
        createIndex(
            "test_offset_range",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1") // Start from offset 1
                .put("ingestion_source.pointer.end.type", "end_by_offset")
                .put("ingestion_source.pointer.end.value", "3") // End at offset 3
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        // Wait for ingestion to complete and poller to be CLOSED
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState("test_offset_range");
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.pollerState().equalsIgnoreCase("closed"));
        });

        // Now query and validate the results
        refresh("test_offset_range");
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch("test_offset_range").setQuery(query).get();

        assertThat("Should ingest exactly 3 messages (offsets 1, 2, 3)",
            response.getHits().getTotalHits().value(), is(3L));

        // Verify the ingestion stats
        PollingIngestStats stats = client().admin().indices().prepareStats("test_offset_range")
            .get().getIndex("test_offset_range").getShards()[0].getPollingIngestStats();
        assertNotNull(stats);
        assertThat("Should have polled exactly 3 messages",
            stats.getConsumerStats().totalPolledCount(), is(3L));
    }

    public void testKafkaIngestionWithOffsetInFuture() throws Exception {
        // Produce 3 messages first
        for (int i = 1; i <= 3; i++) {
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i));
        }

        // Create index with end offset in future (offset 10, but we only have 3 messages at offsets 0,1,2)
        createIndex(
            "test_offset_future",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.pointer.end.type", "end_by_offset")
                .put("ingestion_source.pointer.end.value", "10") // Future offset
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        // Produce more messages to reach and exceed the end offset
        for (int i = 4; i <= 12; i++) { // Produce enough to go beyond offset 10
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i));
        }

        // Wait for ingestion to complete and poller to be CLOSED
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState("test_offset_future");
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.pollerState().equalsIgnoreCase("closed"));
        });

        // Now query and validate the results
        refresh("test_offset_future");
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch("test_offset_future").setQuery(query).get();

        // Should ingest messages from offset 0 to 10 (inclusive), so 11 messages total
        assertThat("Should ingest messages up to offset 10",
            response.getHits().getTotalHits().value(), is(11L));
    }

    public void testKafkaIngestionWithTimestampRange() throws Exception {
        long baseTimestamp = System.currentTimeMillis();
        long startTimestamp = baseTimestamp + 10000; // 10 seconds from now
        long endTimestamp = baseTimestamp + 30000;   // 30 seconds from now

        // Produce messages with different timestamps
        produceData("1", "name1", "21", baseTimestamp, "index");           // Before range
        produceData("2", "name2", "22", startTimestamp + 5000, "index");   // Within range
        produceData("3", "name3", "23", startTimestamp + 10000, "index");  // Within range
        produceData("4", "name4", "24", startTimestamp + 15000, "index");  // Within range
        produceData("5", "name5", "25", endTimestamp + 5000, "index");     // After range

        // Create index with timestamp range
        createIndex(
            "test_timestamp_range",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_timestamp")
                .put("ingestion_source.pointer.init.reset.value", String.valueOf(startTimestamp))
                .put("ingestion_source.pointer.end.type", "end_by_timestamp")
                .put("ingestion_source.pointer.end.value", String.valueOf(endTimestamp))
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        // Wait for ingestion to complete and poller to be CLOSED
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState("test_timestamp_range");
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.pollerState().equalsIgnoreCase("closed"));
        });

        // Now query and validate the results
        refresh("test_timestamp_range");
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch("test_timestamp_range").setQuery(query).get();

        assertThat("Should ingest exactly 3 messages within timestamp range",
            response.getHits().getTotalHits().value(), is(3L));

        // Verify the correct messages were ingested (ages 22, 23, 24)
        RangeQueryBuilder ageQuery = new RangeQueryBuilder("age").gte(22).lte(24);
        SearchResponse ageResponse = client().prepareSearch("test_timestamp_range").setQuery(ageQuery).get();
        assertThat("Should have messages with ages 22-24",
            ageResponse.getHits().getTotalHits().value(), is(3L));

        // Verify messages outside the range were not ingested
        RangeQueryBuilder beforeQuery = new RangeQueryBuilder("age").lt(22);
        SearchResponse beforeResponse = client().prepareSearch("test_timestamp_range").setQuery(beforeQuery).get();
        assertThat("Should not have messages before timestamp range",
            beforeResponse.getHits().getTotalHits().value(), is(0L));

        RangeQueryBuilder afterQuery = new RangeQueryBuilder("age").gt(24);
        SearchResponse afterResponse = client().prepareSearch("test_timestamp_range").setQuery(afterQuery).get();
        assertThat("Should not have messages after timestamp range",
            afterResponse.getHits().getTotalHits().value(), is(0L));
    }

    public void testKafkaIngestionWithTimestampInFuture() throws Exception {
        long baseTimestamp = System.currentTimeMillis();
        long futureTimestamp = baseTimestamp + 60000; // 1 minute in future

        // Produce initial messages with current timestamps
        for (int i = 1; i <= 3; i++) {
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i), baseTimestamp + (i * 1000), "index");
        }

        // Produce more messages with timestamps before the future end timestamp
        long midTimestamp = baseTimestamp + 30000; // 30 seconds from base
        for (int i = 4; i <= 6; i++) {
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i), midTimestamp + (i * 1000), "index");
        }

        // Produce messages with timestamps beyond the future end timestamp
        long beyondFutureTimestamp = futureTimestamp + 10000; // Beyond the end timestamp
        for (int i = 7; i <= 9; i++) {
            produceData(String.valueOf(i), "name" + i, String.valueOf(20 + i), beyondFutureTimestamp + (i * 1000), "index");
        }

        // Create index with end timestamp in future
        createIndex(
            "test_timestamp_future",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.pointer.end.type", "end_by_timestamp")
                .put("ingestion_source.pointer.end.value", String.valueOf(futureTimestamp))
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        // Wait for ingestion to complete and poller to be CLOSED
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState("test_timestamp_future");
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.pollerState().equalsIgnoreCase("closed"));
        });

        // Now query and validate the results
        refresh("test_timestamp_future");
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch("test_timestamp_future").setQuery(query).get();

        // Should have ingested only the 6 messages with timestamps before the future end timestamp
        assertThat("Should ingest only messages before future end timestamp",
            response.getHits().getTotalHits().value(), is(6L));

        // Verify the correct age range was ingested (ages 21-26, not 27-29)
        RangeQueryBuilder validAgeQuery = new RangeQueryBuilder("age").gte(21).lte(26);
        SearchResponse validAgeResponse = client().prepareSearch("test_timestamp_future").setQuery(validAgeQuery).get();
        assertThat("Should have messages with ages 21-26",
            validAgeResponse.getHits().getTotalHits().value(), is(6L));

        // Verify messages beyond the end timestamp were not ingested
        RangeQueryBuilder beyondQuery = new RangeQueryBuilder("age").gte(27).lte(29);
        SearchResponse beyondResponse = client().prepareSearch("test_timestamp_future").setQuery(beyondQuery).get();
        assertThat("Should not have messages beyond end timestamp",
            beyondResponse.getHits().getTotalHits().value(), is(0L));
    }
}
