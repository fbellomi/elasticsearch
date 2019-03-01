package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.ObjectScatterSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.procedures.LongProcedure;
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
    private ObjectArray buckets;

    private final BigArrays bigArrays;

    private BytesRefSet interner;

    CountCollector(final BigArrays arrays)
    {
        bigArrays = arrays;
        buckets = arrays.newObjectArray(1);
    }

    BytesRefSet getInterner()
    {
        if (interner == null)
            interner = new BytesRefSet();
        return interner;
    }

    private Object safeGet(final long bucket)
    {
        if (bucket >= buckets.size())
            return null;
        return buckets.get(bucket);
    }

    static CountCollector readSingletonFrom(final StreamInput in) throws IOException
    {
        final CountCollector c = new CountCollector(BigArrays.NON_RECYCLING_INSTANCE);
        final boolean numbers = in.readBoolean();
        if (numbers)
        {
            final int count = in.readInt();
            if (count > 0)
            {
                final LongScatterSet bs = c.getCreateInt(0);
                for (int i = 0; i < count; i++)
                    bs.add(in.readLong());
            }
        }
        else
        {
            final int count = in.readInt();
            if (count > 0)
            {
                final ObjectScatterSet<BytesRef> bs = c.getCreate(0);
                for (int i = 0; i < count; i++)
                    bs.add(c.readBytesRef(in));
            }
        }
        return c;
    }

    int cardinality(final long bucket)
    {
        final Object bs = safeGet(bucket);
        if (bs == null)
            return 0;
        else if (bs instanceof BytesRefSet)
            return ((BytesRefSet) bs).size();
        else // if (bs instanceof LongScatterSet)
            return ((LongScatterSet)bs).size();
    }

    class BytesRefSet extends ObjectScatterSet<BytesRef>
    {
        BytesRef addClone(final BytesRef key)
        {
            if (((key) == null)) {
              hasEmptyKey = true;
              return null;
            } else {
              final Object [] keys = this.keys;
              final int mask = this.mask;
              int slot = hashKey(key) & mask;

              Object existing;
              while (!((existing = keys[slot]) == null)) {
                if (this.equals(existing,  key)) {
                  return (BytesRef) existing;
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
              return nk;
            }
        }

    }

    @SuppressWarnings("unchecked")
    BytesRefSet getCreate(final long bucket)
    {
        BytesRefSet r = (BytesRefSet) safeGet(bucket);
        if (r == null)
        {
            buckets = bigArrays.grow(buckets, bucket+1);
            r = new BytesRefSet();
            buckets.set(bucket, r);
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    LongScatterSet getCreateInt(final long bucket)
    {
        LongScatterSet r = (LongScatterSet) safeGet(bucket);
        if (r == null)
        {
            buckets = bigArrays.grow(buckets, bucket+1);
            r = new LongScatterSet();
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
        final Object br = safeGet(bucket);
        return br == null ||
            (br instanceof BytesRefSet && ((BytesRefSet) br).isEmpty()) ||
            (br instanceof LongScatterSet && ((LongScatterSet) br).isEmpty());
    }

    CountCollector singleton(final long bucket)
    {
        final CountCollector count = new CountCollector(BigArrays.NON_RECYCLING_INSTANCE);
        //noinspection unchecked
        count.buckets.set(0, buckets.get(bucket));
        return count;
    }

    @SuppressWarnings("SameParameterValue")
    void writeSingletonTo(final long bucket, final StreamOutput out) throws IOException
    {
        final Object z = buckets.get(bucket);
        if (z instanceof LongScatterSet)
        {
            out.writeBoolean(true);
            final LongScatterSet bs = (LongScatterSet) z;
            out.writeInt(bs.size());
            try
            {
                bs.forEach((LongProcedure) value -> {
                    try
                    {
                        out.writeLong(value);
                    }
                    catch (final IOException e)
                    {
                        throw new InternalError(e);
                    }
                });
            }
            catch (InternalError e)
            {
                throw (IOException) Objects.requireNonNull(e.getCause());
            }
        }
        else
        {
            out.writeBoolean(false);
            if (z == null)
                out.writeInt(0);
            else
            {
                //noinspection unchecked
                final ObjectScatterSet<BytesRef> bs = (ObjectScatterSet<BytesRef>) z;
                out.writeInt(bs.size());
                for (final ObjectCursor<BytesRef> i : bs)
                    out.writeBytesRef(i.value);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    int hashCode(final long bucket)
    {
        final Object bs = safeGet(bucket);
        return bs == null ? 0 : bs.hashCode();
    }

    public boolean equals(final long bucket, final CountCollector counts)
    {
        final Object bs = safeGet(bucket);
        final Object bs2 = counts.safeGet(bucket);
        return Objects.equals(bs, bs2);
    }

    public void merge(final long bucket, final CountCollector other, final long otherBucket)
    {
        final Object b = safeGet(bucket);
        final Object b2 = other.safeGet(otherBucket);
        if (b == null)
        {
            if (b2 instanceof BytesRefSet)
                getCreate(bucket).addAll((BytesRefSet) b2);
            else if (b2 instanceof LongScatterSet)
                getCreateInt(bucket).addAll((LongScatterSet) b2);
        }
        else if (b2 != null)
        {
            if (b instanceof BytesRefSet)
                ((BytesRefSet) b).addAll((BytesRefSet) b2);
            else if (b instanceof LongScatterSet)
                ((LongScatterSet) b).addAll((LongScatterSet) b2);
        }
    }
}
