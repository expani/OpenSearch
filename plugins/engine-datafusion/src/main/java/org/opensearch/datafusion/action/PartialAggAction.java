/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Stream Transport action for partial aggregation on DataNodes.
 */
public class PartialAggAction extends ActionType<PartialAggAction.Response> {
    public static final PartialAggAction INSTANCE = new PartialAggAction();
    public static final String NAME = "indices:data/read/partial_agg";

    private PartialAggAction() {
        super(NAME, Response::new);
    }

    /** Placeholder — actual data streams as native Arrow batches. */
    public static class Response extends ActionResponse {
        public Response() {}
        public Response(StreamInput in) throws IOException { super(in); }
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
