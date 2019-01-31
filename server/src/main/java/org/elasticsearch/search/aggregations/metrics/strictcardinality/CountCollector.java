package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import com.carrotsearch.hppc.LongObjectScatterMap;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectScatterSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public final class CountCollector
{
    private final LongObjectScatterMap<ObjectScatterSet<BytesRef>> buckets = new LongObjectScatterMap<>();

    static CountCollector readSingletonFrom(final StreamInput in) throws IOException
    {
        final CountCollector c = new CountCollector();
        final ObjectScatterSet<BytesRef> bs = c.getCreate(0);
        final int count = in.readInt();
        for (int i = 0; i < count; i++)
            bs.add(in.readBytesRef());
        return c;
    }

    int cardinality(final long bucket)
    {
        final ObjectHashSet<BytesRef> bs = buckets.get(bucket);
        return bs == null ? 0 : bs.size();
    }

    ObjectScatterSet<BytesRef> getCreate(final long bucket)
    {
        ObjectScatterSet<BytesRef> r = buckets.get(bucket);
        if (r == null)
        {
            r = new ObjectScatterSet<>(1024);
            buckets.put(bucket, r);
        }
        return r;
    }

    boolean isEmptyBucket(final long bucket)
    {
        final ObjectHashSet<BytesRef> br = buckets.get(bucket);
        return br == null || br.isEmpty();
    }

    CountCollector singleton(final long bucket)
    {
        final CountCollector count = new CountCollector();
        count.buckets.put(0, buckets.get(bucket));
        return count;
    }

    @SuppressWarnings("SameParameterValue")
    void writeSingletonTo(final long bucket, final StreamOutput out) throws IOException
    {
        final ObjectScatterSet<BytesRef> bs = getCreate(bucket);
        out.writeInt(bs.size());
        for (final ObjectCursor<BytesRef> i : bs)
            out.writeBytesRef(i.value);
    }

    @SuppressWarnings("SameParameterValue")
    int hashCode(final long bucket)
    {
        final ObjectHashSet<BytesRef> bs = buckets.get(bucket);
        return bs == null ? 0 : bs.hashCode();
    }

    public boolean equals(final long bucket, final CountCollector counts)
    {
        final ObjectHashSet<BytesRef> bs = buckets.get(bucket);
        final ObjectHashSet<BytesRef> bs2 = counts.buckets.get(bucket);
        return Objects.equals(bs, bs2);
    }

    public void merge(final long bucket, final CountCollector other, final long otherBucket)
    {
        final ObjectHashSet<BytesRef> bs = getCreate(bucket);
        final ObjectHashSet<BytesRef> bs2 = other.buckets.get(otherBucket);
        if (bs2 != null)
            bs.addAll(bs2);
    }
}
