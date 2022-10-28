package io.trino.plugin.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.udf.BitmapOperators.serialize;
import static io.trino.plugin.udf.BitmapOperators.deserialize;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static java.util.Objects.requireNonNull;

/**
 * bitmap of udf
 *
 * @author di_deng@droidhang.com
 */
public class BitmapFunction
{

    @Description("covert a positive number to bitmap, return byte array")
    @ScalarFunction(value = "to_bitmap")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toBitmap(@SqlType(INTEGER) long value)
    {
        checkArgument((value < Integer.MAX_VALUE && value >= 0), "必须为32位正整数: %s", value);
        try {
            RoaringBitmap rb = BitmapOperators.toBitmap(value);
            return Slices.wrappedBuffer(serialize(rb));
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, value + "无法转化成bitmap");
        }
    }

    @Description("return bitmap`s cardinality")
    @ScalarFunction(value = "bitmap_count")
    @SqlType(StandardTypes.BIGINT)
    public static long bitmapCount(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        requireNonNull(input, "不支持输入为null值");

        try {
            return BitmapOperators.bitmapCount(input);
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法计算输入的bitmap大小");
        }
    }

    @Description("covert bitmap to array")
    @ScalarFunction("bitmap_to_array")
    @SqlType("array(bigint)")
    public static Block bitmapToArray(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        requireNonNull(input, "不支持输入为null值");

        try {
            RoaringBitmap rb = deserialize(input);
            BlockBuilder output = BIGINT.createBlockBuilder(null, rb.getCardinality());
            for(int i : rb) {
                BIGINT.writeLong(output, i);
            }
            return output.build();
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法将bitmap转成array");
        }
    }

    @Description("calculate the intersection of two input bitmaps and return the new bitmap")
    @ScalarFunction(value = "bitmap_and")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice bitmapAnd(@SqlType(StandardTypes.VARBINARY) Slice left,
                                  @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        checkArgument((left != null && right != null), "不支持输入为null值");

        try {
            RoaringBitmap rb = BitmapOperators.and(left, right);
            return Slices.wrappedBuffer(serialize(rb));
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法计算输入两个bitmap的交集");
        }
    }

    @Description("calculate the union of two input bitmaps and return a new bitmap")
    @ScalarFunction(value = "bitmap_or")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice bitmapOr(@SqlType(StandardTypes.VARBINARY) Slice left,
                                 @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        checkArgument((left != null && right != null), "不支持输入为null值");

        try {
            RoaringBitmap rb = BitmapOperators.or(left, right);
            return Slices.wrappedBuffer(serialize(rb));
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法计算输入两个bitmap的并集");
        }
    }

    @Description("calculate the set after lhs minus rhs, and return the new bitmap")
    @ScalarFunction(value = "bitmap_andnot")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice bitmapAndNot(@SqlType(StandardTypes.VARBINARY) Slice left,
                                    @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        checkArgument((left != null && right != null), "不支持输入为null值");

        try {
            RoaringBitmap rb = BitmapOperators.andNot(left, right);
            return Slices.wrappedBuffer(serialize(rb));
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法计算输入两个bitmap的差集");
        }
    }

    @Description("calculate whether the input value is in the bitmap column, and return a Boolean value")
    @ScalarFunction(value = "bitmap_contain")
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean bitmapContain(@SqlType(StandardTypes.VARBINARY) Slice input,
                                        @SqlType(StandardTypes.BIGINT) long value)
    {
        requireNonNull(input, "不支持输入为null值");

        try {
            return BitmapOperators.contain(input, value);
        } catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "无法计算输入值是否存在于bitmap中");
        }
    }



}
