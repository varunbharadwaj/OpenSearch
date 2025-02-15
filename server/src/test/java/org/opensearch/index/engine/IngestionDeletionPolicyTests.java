/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class IngestionDeletionPolicyTests extends CombinedDeletionPolicyTests {

    public void testAcquireAndReleaseIndexCommit() throws Exception {
        IndexCommit commit1 = mock(IndexCommit.class);
        IndexCommit commit2 = mock(IndexCommit.class);

        IngestionDeletionPolicy deletionPolicy = createIngestionDeletionPolicy();
        deletionPolicy.onInit(Arrays.asList(commit1, commit2));
        assertTrue(deletionPolicy.getSnapshottedCommits().isEmpty());

        IndexCommit snapshotCommit = deletionPolicy.acquireIndexCommit();
        assertFalse(deletionPolicy.getSnapshottedCommits().isEmpty());

        IndexCommit commit3 = mock(IndexCommit.class);
        deletionPolicy.onCommit(Arrays.asList(commit1, commit2, commit3));
        verify(commit2, never()).delete();

        deletionPolicy.releaseCommit(snapshotCommit);
        IndexCommit commit4 = mock(IndexCommit.class);
        deletionPolicy.onCommit(Arrays.asList(commit2, commit3, commit4));
        assertTrue(deletionPolicy.getSnapshottedCommits().isEmpty());
        verify(commit2, times(1)).delete();
        verify(commit3, times(1)).delete();
    }

    private IngestionDeletionPolicy createIngestionDeletionPolicy() {
        return new IngestionDeletionPolicy(logger) {
            @Override
            protected int getDocCountOfCommit(IndexCommit indexCommit) {
                return between(0, 1000);
            }
        };
    }
}
