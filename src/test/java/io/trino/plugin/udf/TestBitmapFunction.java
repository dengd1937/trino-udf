package io.trino.plugin.udf;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.udf.SqlVarbinaryTestingUtil.*;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class TestBitmapFunction
        extends AbstractTestUDFs
{
    @Test
    public void testToBitmap() throws IOException
    {
        assertFunction("to_bitmap(0)", VARBINARY, sqlVarbinaryToBitmap(0));
    }

    @Test
    public void testBitmapCount()
    {
        assertFunction("bitmap_count(to_bitmap(0))", BIGINT, 1L);
    }

    @Test
    public void testBitmapToArray()
    {
        assertFunction("bitmap_to_array(to_bitmap(1))", new ArrayType(BIGINT), ImmutableList.of(1L));
    }

    @Test
    public void testBitmapAnd() throws IOException
    {
        assertFunction("bitmap_and(to_bitmap(1), to_bitmap(1))", VARBINARY, sqlVarbinaryToBitmap(1));
        assertFunction("bitmap_and(to_bitmap(1), to_bitmap(0))", VARBINARY, sqlVarbinaryEmptyBitmap());
    }

    @Test
    public void testBitmapOr() throws IOException
    {
        assertFunction("bitmap_or(to_bitmap(1), to_bitmap(1))", VARBINARY, sqlVarbinaryToBitmap(1));
        assertFunction("bitmap_or(to_bitmap(1), to_bitmap(0))", VARBINARY, sqlVarbinaryToBitmap(0, 1));
    }

    @Test
    public void testBitmapAndOr() throws IOException
    {
        assertFunction("bitmap_andnot(to_bitmap(1), to_bitmap(1))", VARBINARY, sqlVarbinaryEmptyBitmap());
        assertFunction("bitmap_andnot(to_bitmap(1), to_bitmap(0))", VARBINARY, sqlVarbinaryToBitmap(1));
    }

    @Test
    public void testBitmapContain()
    {
        assertFunction("bitmap_contain(to_bitmap(1), 1)", BOOLEAN, true);
        assertFunction("bitmap_contain(to_bitmap(1), 0)", BOOLEAN, false);
        assertFunction("bitmap_contain(to_bitmap(1), -1)", BOOLEAN, false);
    }

}
