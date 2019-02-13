package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import com.carrotsearch.hppc.IntScatterSet;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

public final class IntValueCollector extends LeafBucketCollector
{
    private final SortedNumericDocValues values;

    private final CountCollector counts;

    IntValueCollector(final SortedNumericDocValues values, final CountCollector counts)
    {
        this.values = values;
        this.counts = counts;
    }

    @Override
    public void collect(final int doc, final long bucket) throws IOException
    {
        if (values.advanceExact(doc)) {
            final int valueCount = values.docValueCount();
            final IntScatterSet z = counts.getCreateInt(bucket);
            for (int i = 0; i < valueCount; i++) {
                z.add((int) values.nextValue());
            }
        }
    }
}
