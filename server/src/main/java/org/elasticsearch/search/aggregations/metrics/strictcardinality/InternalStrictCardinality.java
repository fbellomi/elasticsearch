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

package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class InternalStrictCardinality extends InternalNumericMetricsAggregation.SingleValue implements StrictCardinality
{
    private final CountCollector counts;

    InternalStrictCardinality(String name, CountCollector counts, List<PipelineAggregator> pipelineAggregators,
                              Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.counts = counts;
    }

    /**
     * Read from a stream.
     */
    public InternalStrictCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = CountCollector.readSingletonFrom(in);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeSingletonTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return StrictCardinalityAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalStrictCardinality reduced = null;
        for (final InternalAggregation aggregation : aggregations) {
            final InternalStrictCardinality cardinality = (InternalStrictCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new InternalStrictCardinality(name, new CountCollector(), pipelineAggregators(), getMetaData());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalStrictCardinality other) {
        assert counts != null && other != null;
        counts.merge(0, other.counts, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE.getPreferredName(), cardinality);
        return builder;
    }

    @Override
    protected int doHashCode() {
        return counts.hashCode(0);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalStrictCardinality other = (InternalStrictCardinality) obj;
        return counts.equals(0, other.counts);
    }
}

