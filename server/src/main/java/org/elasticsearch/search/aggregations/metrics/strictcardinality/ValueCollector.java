package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

public final class ValueCollector   extends LeafBucketCollector implements Releasable
{
    private final SortedBinaryDocValues values;

    private final CountCollector counts;

    ValueCollector(final SortedBinaryDocValues values, final CountCollector counts)
    {
        this.values = values;
        this.counts = counts;
    }

    @Override
    public void close()
    {
        // no-op
    }

    @Override
    public void collect(final int doc, final long bucket) throws IOException
    {
        if (values.advanceExact(doc)) {
            final int valueCount = values.docValueCount();
            final CountCollector.BytesRefSet z = counts.getCreate(bucket);
            for (int i = 0; i < valueCount; i++) {
                z.addClone(values.nextValue());
            }
        }
    }
}
