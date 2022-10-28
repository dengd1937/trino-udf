package io.trino.plugin.udf.aggregation;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.udf.BitmapOperators;
import io.trino.plugin.udf.aggregation.state.SliceState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static io.trino.plugin.udf.BitmapOperators.serialize;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/**
 * 计算一组bitmap值的交集
 *
 * @author di_deng@droidhang.com
 */
@AggregationFunction("bitmap_intersect")
public final class BitmapIntersectAggregation {
    public BitmapIntersectAggregation() {
    }

    @InputFunction
    public static void bitmapAnd(@AggregationState SliceState state, @SqlType(StandardTypes.VARBINARY) Slice value) {
        try {
            if (state.getSlice() == null) {
                state.setSlice(value);
            } else {
                RoaringBitmap rb = BitmapOperators.and(state.getSlice(), value);
                state.setSlice(Slices.wrappedBuffer(serialize(rb)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @CombineFunction
    public static void combine(@AggregationState SliceState state, @AggregationState SliceState otherState) {
        try {
            if (state.getSlice() == null) {
                state.setSlice(otherState.getSlice());
            } else if (otherState.getSlice() != null) {
                RoaringBitmap rb = BitmapOperators.and(state.getSlice(), otherState.getSlice());
                state.setSlice(Slices.wrappedBuffer(serialize(rb)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState SliceState state, BlockBuilder out) {
        SliceState.write(VARBINARY, state, out);
    }

}
