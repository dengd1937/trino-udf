package io.trino.plugin.udf.aggregation.state;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.type.Type;

public interface SliceState
        extends AccumulatorState
{
    Slice getSlice();

    void setSlice(Slice value);

    static void write(Type type, SliceState state, BlockBuilder out)
    {
        if (state.getSlice() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getSlice());
        }
    }
}
