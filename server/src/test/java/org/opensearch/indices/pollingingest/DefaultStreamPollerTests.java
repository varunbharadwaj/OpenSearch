/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultStreamPollerTests extends OpenSearchTestCase {
    private DefaultStreamPoller poller;
    private FakeIngestionSource.FakeIngestionConsumer fakeConsumer;
    private MessageProcessorRunnable processorRunnable;
    private MessageProcessorRunnable.MessageProcessor processor;
    private List<byte[]> messages;
    private Set<IngestionShardPointer> persistedPointers;
    private final int sleepTime = 300;
    private DropIngestionErrorStrategy errorStrategy;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messages = new ArrayList<>();
        ;
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"2\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        fakeConsumer = new FakeIngestionSource.FakeIngestionConsumer(messages, 0);
        processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        errorStrategy = new DropIngestionErrorStrategy("ingestion_source");
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, errorStrategy);
        persistedPointers = new HashSet<>();
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE,
            errorStrategy
        );
    }

    @After
    public void tearDown() throws Exception {
        if (!poller.isClosed()) {
            poller.close();
        }
        super.tearDown();
    }

    public void testPauseAndResume() throws InterruptedException {
        poller.pause();
        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
        // no messages are processed
        verify(processor, never()).process(any(), any());

        poller.resume();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        assertFalse(poller.isPaused());
        // 2 messages are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testSkipProcessed() throws InterruptedException {
        messages.add("{\"name\":\"cathy\", \"age\": 21}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"name\":\"danny\", \"age\": 31}".getBytes(StandardCharsets.UTF_8));
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(1));
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(2));
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE,
            errorStrategy
        );
        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        // 2 messages are processed, 2 messages are skipped
        verify(processor, times(2)).process(any(), any());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getMaxPersistedPointer());
    }

    public void testCloseWithoutStart() {
        poller.close();
        assertTrue(poller.isClosed());
    }

    public void testClose() throws InterruptedException {
        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        poller.close();
        assertTrue(poller.isClosed());
        assertEquals(DefaultStreamPoller.State.CLOSED, poller.getState());
    }

    public void testResetStateEarliest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(1),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.EARLIEST,
            errorStrategy
        );

        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run

        // 2 messages are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testResetStateLatest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.LATEST,
            errorStrategy
        );

        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        // no messages processed
        verify(processor, never()).process(any(), any());
        // reset to the latest
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getBatchStartPointer());
    }

    public void testStartPollWithoutStart() {
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is not started!", e.getMessage());
        }
    }

    public void testStartClosedPoller() throws InterruptedException {
        poller.start();
        Thread.sleep(sleepTime);
        poller.close();
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is closed!", e.getMessage());
        }
    }

    public void testDropErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch1 = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    2,
                    100
                );
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch2 = fakeConsumer.readNext(fakeConsumer.nextPointer(), 2, 100);
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyLong(), anyInt())).thenThrow(new RuntimeException("message1 poll failed"))
            .thenReturn(readResultsBatch1)
            .thenThrow(new RuntimeException("message3 poll failed"))
            .thenReturn(readResultsBatch2)
            .thenReturn(Collections.emptyList());

        IngestionErrorStrategy errorStrategy = spy(new DropIngestionErrorStrategy("ingestion_source"));
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            mockConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE,
            errorStrategy
        );
        poller.start();
        Thread.sleep(sleepTime);

        verify(errorStrategy, times(2)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.POLLING));
        verify(processor, times(4)).process(any(), any());
    }

    public void testBlockErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch1 = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    2,
                    100
                );
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch2 = fakeConsumer.readNext(fakeConsumer.nextPointer(), 2, 100);
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyLong(), anyInt())).thenThrow(new RuntimeException("message1 poll failed"))
            .thenReturn(readResultsBatch1)
            .thenReturn(readResultsBatch2)
            .thenReturn(Collections.emptyList());

        IngestionErrorStrategy errorStrategy = spy(new BlockIngestionErrorStrategy("ingestion_source"));
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            mockConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE,
            errorStrategy
        );
        poller.start();
        Thread.sleep(sleepTime);

        verify(errorStrategy, times(1)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.POLLING));
        verify(processor, never()).process(any(), any());
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
    }

    public void testProcessingErrorWithBlockErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));

        doThrow(new RuntimeException("Error processing update")).when(processor).process(any(), any());
        BlockIngestionErrorStrategy mockErrorStrategy = spy(new BlockIngestionErrorStrategy("ingestion_source"));
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, mockErrorStrategy);

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE,
            mockErrorStrategy
        );
        poller.start();
        Thread.sleep(sleepTime);

        verify(mockErrorStrategy, times(1)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.PROCESSING));
        verify(processor, times(1)).process(any(), any());
        // poller will continue to poll if an error is encountered during message processing but will be blocked by
        // the write to blockingQueue
        assertEquals(DefaultStreamPoller.State.POLLING, poller.getState());
    }
}
