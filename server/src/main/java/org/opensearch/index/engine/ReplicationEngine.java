/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.seqno.LocalCheckpointTracker;

import java.io.IOException;

@PublicApi(since = "1.0.0")
public abstract class ReplicationEngine extends Engine {

    public ReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    public abstract void cleanUnreferencedFiles() throws IOException;

    public abstract void updateSegments(final SegmentInfos infos) throws IOException;

    protected abstract LocalCheckpointTracker getLocalCheckpointTracker();
}
