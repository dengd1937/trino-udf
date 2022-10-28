package io.trino.plugin.udf;

import io.airlift.slice.Slice;
import io.trino.spi.type.SqlVarbinary;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

import static com.google.common.base.Preconditions.checkArgument;

public final class SqlVarbinaryTestingUtil {

    private SqlVarbinaryTestingUtil() {}


    public static SqlVarbinary sqlVarbinaryToBitmap(long value) throws IOException
    {
        checkArgument((value < Integer.MAX_VALUE && value >= 0), "必须为32位正整数: %s", value);
        RoaringBitmap rb = RoaringBitmap.bitmapOf((int) value);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        rb.serialize(new DataOutputStream(bos));
        return new SqlVarbinary(bos.toByteArray());
    }

    public static SqlVarbinary sqlVarbinaryToBitmap(long... values) throws IOException
    {
        RoaringBitmap rb = new RoaringBitmap();
        for (long value : values) {
            checkArgument((value < Integer.MAX_VALUE && value >= 0), "必须为32位正整数: %s", value);
            rb.add((int) value);
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        rb.serialize(new DataOutputStream(bos));
        return new SqlVarbinary(bos.toByteArray());
    }

    public static SqlVarbinary sqlVarbinaryEmptyBitmap() throws IOException
    {
        RoaringBitmap rb = new RoaringBitmap();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        rb.serialize(new DataOutputStream(bos));
        return new SqlVarbinary(bos.toByteArray());
    }

    public static byte[] bitmapToBytes(RoaringBitmap rb) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        rb.serialize(new DataOutputStream(bos));
        return bos.toByteArray();
    }

}
