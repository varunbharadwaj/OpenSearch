/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.Closeable;
import java.util.Locale;

/**
 * A poller for reading messages from an ingestion shard. This is used in the ingestion engine.
 */
public interface StreamPoller extends Closeable, ClusterStateListener {

    String BATCH_START = "batch_start";

    /**
     * Start the poller
     */
    void start();;

    /**
     * Pause the poller
     */
    void pause();

    /**
     * Resume the poller polling
     */
    void resume();

    /**
     * @return if the poller is paused
     */
    boolean isPaused();

    /**
     * Check if the poller is closed
     */
    boolean isClosed();

    /**
     * Get the pointer to the start of the current batch of messages.
     */
    IngestionShardPointer getBatchStartPointer();

    PollingIngestStats getStats();

    IngestionErrorStrategy getErrorStrategy();

    State getState();

    /**
     * Update the error strategy for the poller.
     */
    void updateErrorStrategy(IngestionErrorStrategy errorStrategy);

    /**
     * Returns if write block is active for the poller.
     */
    boolean isWriteBlockEnabled();

    /**
     * Sets write block status for the poller.
     */
    void setWriteBlockEnabled(boolean isWriteBlockEnabled);

    IngestionShardConsumer getConsumer();

    /**
     * Update the end point settings for the poller
     * @param endPointType the new end point type
     * @param endPointValue the new end point value
     */
    void updateIngestionEndPoint(EndState endPointType, String endPointValue);

    /**
     * A state to indicate the current state of the poller
     */
    enum State {
        NONE,
        CLOSED,
        PAUSED,
        POLLING,
        PROCESSING,
    }

    /**
     *  A reset state to indicate how to reset the pointer
     */
    @ExperimentalApi
    enum ResetState {
        EARLIEST,
        LATEST,
        RESET_BY_OFFSET,
        RESET_BY_TIMESTAMP,
        NONE,
    }

    /**
     *  EndState for the poller defining when to stop polling.
     */
    @ExperimentalApi
    enum EndState {
        NONE,
        END_BY_OFFSET,
        END_BY_TIMESTAMP;

        public static EndState parseFromString(String endState) {
            try {
                return EndState.valueOf(endState.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid StreamPoller end state: " + endState, e);
            }
        }
    }
}
