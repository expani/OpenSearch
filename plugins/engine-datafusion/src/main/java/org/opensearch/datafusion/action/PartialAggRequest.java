/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Request for partial aggregation — carries Substrait plan + shard routing.
 */
public class PartialAggRequest extends ActionRequest {
    private final String indexName;
    private final ShardId shardId;
    private final byte[] substraitBytes;

    public PartialAggRequest(String indexName, ShardId shardId, byte[] substraitBytes) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.substraitBytes = substraitBytes;
    }

    public PartialAggRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.shardId = new ShardId(in);
        this.substraitBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        shardId.writeTo(out);
        out.writeByteArray(substraitBytes);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getIndexName() { return indexName; }
    public ShardId getShardId() { return shardId; }
    public byte[] getSubstraitBytes() { return substraitBytes; }
}
