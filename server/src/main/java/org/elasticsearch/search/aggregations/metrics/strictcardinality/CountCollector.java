package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectScatterSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;

import java.io.IOException;
import java.util.Objects;

public final class CountCollector implements Releasable
{
    private ObjectArray<BytesRefSet> buckets;

    private final BigArrays bigArrays;

    CountCollector(final BigArrays arrays)
    {
        bigArrays = arrays;
        buckets = arrays.newObjectArray(1);
    }

    private BytesRefSet safeGet(final long bucket)
    {
        if (bucket >= buckets.size())
            return null;
        return buckets.get(bucket);
    }

    static CountCollector readSingletonFrom(final StreamInput in) throws IOException
    {
        final CountCollector c = new CountCollector(BigArrays.NON_RECYCLING_INSTANCE);
        final ObjectScatterSet<BytesRef> bs = c.getCreate(0);
        final int count = in.readInt();
        for (int i = 0; i < count; i++)
            bs.add(c.readBytesRef(in));
        return c;
    }

    int cardinality(final long bucket)
    {
        final ObjectHashSet<BytesRef> bs = safeGet(bucket);
        return bs == null ? 0 : bs.size();
    }

    class BytesRefSet extends ObjectScatterSet<BytesRef>
    {
        BytesRefSet(final int expectedElements)
        {
            super(expectedElements);
        }

        void addClone(final BytesRef key)
        {
            if (((key) == null)) {
              hasEmptyKey = true;
            } else {
              final Object [] keys = this.keys;
              final int mask = this.mask;
              int slot = hashKey(key) & mask;

              Object existing;
              while (!((existing = keys[slot]) == null)) {
                if (this.equals(existing,  key)) {
                  return;
                }
                slot = (slot + 1) & mask;
              }

              final BytesRef nk = cloneBytesRef(key);   // add a clone
              if (assigned == resizeAt) {
                allocateThenInsertThenRehash(slot, nk);
              } else {
                keys[slot] = nk;
              }

              assigned++;
            }
        }

    }

    BytesRefSet getCreate(final long bucket)
    {
        BytesRefSet r = safeGet(bucket);
        if (r == null)
        {
            buckets = bigArrays.grow(buckets, bucket+1);
            final int size = (bucket < 4) ? 16384 : 1024;
            r = new BytesRefSet(size);
            buckets.set(bucket, r);
        }
        return r;
    }

    private static final int byteBlockSize = 16384;

    private byte[] currentBlock;

    private int currentBlockPos;

    private BytesRef cloneBytesRef(final BytesRef source)
    {
        final int length = source.length;
        ensureLength(length);
        System.arraycopy(source.bytes, source.offset, currentBlock, currentBlockPos, length);
        return buildBytesRef(length);
    }

    private BytesRef buildBytesRef(final int length)
    {
        final BytesRef r = new BytesRef(currentBlock, currentBlockPos, length);
        currentBlockPos += length;
        return r;
    }

    private BytesRef readBytesRef(final StreamInput in) throws IOException
    {
        final int length = in.readVInt();
        ensureLength(length);
        in.readBytes(currentBlock, currentBlockPos, length);
        return buildBytesRef(length);
    }

    private void ensureLength(final int length)
    {
        if (currentBlock == null || currentBlockPos + length > byteBlockSize)
        {
            currentBlock = new byte[byteBlockSize];
            currentBlockPos = 0;
        }
    }

    @Override
    public void close()
    {
        currentBlock = null;
        Releasables.close(buckets);
    }

    boolean isEmptyBucket(final long bucket)
    {
        final ObjectHashSet<BytesRef> br = safeGet(bucket);
        return br == null || br.isEmpty();
    }

    CountCollector singleton(final long bucket)
    {
        final CountCollector count = new CountCollector(BigArrays.NON_RECYCLING_INSTANCE);
        count.buckets.set(0, buckets.get(bucket));
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
        final ObjectHashSet<BytesRef> bs = safeGet(bucket);
        return bs == null ? 0 : bs.hashCode();
    }

    public boolean equals(final long bucket, final CountCollector counts)
    {
        final ObjectHashSet<BytesRef> bs = safeGet(bucket);
        final ObjectHashSet<BytesRef> bs2 = counts.safeGet(bucket);
        return Objects.equals(bs, bs2);
    }

    public void merge(final long bucket, final CountCollector other, final long otherBucket)
    {
        final ObjectHashSet<BytesRef> bs = getCreate(bucket);
        final ObjectHashSet<BytesRef> bs2 = other.safeGet(otherBucket);
        if (bs2 != null)
            bs.addAll(bs2);
    }
}
