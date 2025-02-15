/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A variant of {@link CombinedDeletionPolicy} to be used in pull-based ingestion. This policy deletes all older commits
 * except for snapshotted commits.
 *
 * @opensearch.internal
 */
public class IngestionDeletionPolicy extends CombinedDeletionPolicy {
    private volatile IndexCommit lastCommit;
    private volatile SafeCommitInfo safeCommitInfo = SafeCommitInfo.EMPTY;

    // tracks number of references held on snapshotted commits
    private final Map<IndexCommit, Integer> snapshottedCommits;

    public IngestionDeletionPolicy(Logger logger) {
        super(logger, null, null, null);
        this.snapshottedCommits = new HashMap<>();
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        assert commits.isEmpty() == false : "index is opened, but we have no commits";
        onCommit(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        synchronized (this) {
            this.safeCommitInfo = SafeCommitInfo.EMPTY;
            this.lastCommit = commits.get(commits.size() - 1);

            for (int i = commits.size() - 2; i >= 0; i--) {
                if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                    commits.get(i).delete();
                }
            }
        }

        safeCommitInfo = new SafeCommitInfo(0, getDocCountOfCommit(lastCommit));
    }

    synchronized IndexCommit acquireIndexCommit() {
        assert lastCommit != null : "Last commit is not initialized yet";
        snapshottedCommits.merge(lastCommit, 1, Integer::sum); // increase refCount
        return new SnapshotIndexCommit(lastCommit);
    }

    synchronized boolean releaseCommit(final IndexCommit snapshotCommit) {
        final IndexCommit releasingCommit = ((SnapshotIndexCommit) snapshotCommit).delegate;
        assert snapshottedCommits.containsKey(releasingCommit) : "Release non-snapshotted commit;"
            + "snapshotted commits ["
            + snapshottedCommits
            + "], releasing commit ["
            + releasingCommit
            + "]";
        final int refCount = snapshottedCommits.merge(releasingCommit, -1, Integer::sum); // release refCount
        assert refCount >= 0 : "Number of snapshots can not be negative [" + refCount + "]";
        if (refCount == 0) {
            snapshottedCommits.remove(releasingCommit);
        }
        // The commit can be clean up only if no pending snapshot, and it is not the recent commit
        return refCount == 0 && releasingCommit.equals(lastCommit) == false;
    }

    @Override
    protected SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    // visible for testing
    Map<IndexCommit, Integer> getSnapshottedCommits() {
        return snapshottedCommits;
    }
}
