package io.trino.plugin.udf;


import io.airlift.slice.Slice;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

import static java.util.Objects.requireNonNull;

public final class BitmapOperators
{
    public BitmapOperators() {}

    public static byte[] serialize(RoaringBitmap rb) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        rb.serialize(new DataOutputStream(bos));
        return bos.toByteArray();
    }

    public static RoaringBitmap deserialize(Slice input) throws IOException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(input.getBytes());
        RoaringBitmap rb = new RoaringBitmap();
        rb.deserialize(new DataInputStream(bis));
        return rb;
    }

    public static RoaringBitmap toBitmap(long value)
    {
        return RoaringBitmap.bitmapOf((int) value);
    }

    public static RoaringBitmap toBitmap(Slice slice) throws IOException
    {
        requireNonNull(slice, "输入参数不能为null");
        return deserialize(slice);
    }

    public static long bitmapCount(Slice input) throws IOException
    {
        return deserialize(input).getCardinality();
    }

    public static Boolean contain(Slice input, long value) throws IOException
    {
        return deserialize(input).contains((int) value);
    }

    public static RoaringBitmap and(Slice left, Slice right) throws IOException
    {
        RoaringBitmap lrb = deserialize(left);
        if (right != null) {
            RoaringBitmap rrb = deserialize(right);
            lrb.and(rrb);
        }
        return lrb;
    }

    public static RoaringBitmap and(RoaringBitmap left, RoaringBitmap right)
    {
        if (right != null && right.getCardinality() > 0) {
            left.and(right);
        }
        return left;
    }

    public static RoaringBitmap or(Slice left, Slice right) throws IOException
    {
        RoaringBitmap lrb = deserialize(left);
        if (right != null) {
            RoaringBitmap rrb = deserialize(right);
            lrb.or(rrb);
        }
        return lrb;
    }

    public static RoaringBitmap or(RoaringBitmap left, RoaringBitmap right)
    {
        if (right != null && right.getCardinality() > 0) {
            left.or(right);
        }
        return left;
    }

    public static RoaringBitmap andNot(Slice left, Slice right) throws IOException
    {
        RoaringBitmap lrb = deserialize(left);
        if (right != null) {
            RoaringBitmap rrb = deserialize(right);
            lrb.andNot(rrb);
        }
        return lrb;
    }

}
