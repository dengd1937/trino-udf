package io.trino.plugin.udf.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.plugin.udf.UDFPlugin;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.List;

import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static io.trino.plugin.udf.SqlVarbinaryTestingUtil.bitmapToBytes;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class TestBitmapUnionAggregation
    extends AbstractTestAggregationFunction
{
    @BeforeClass
    protected void registerFunctions()
    {
        metadata.addFunctions(
                extractFunctions(new UDFPlugin().getFunctions()));
    }

    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            try {
                byte[] bytes = bitmapToBytes(RoaringBitmap.bitmapOf(i));
                VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        RoaringBitmap rb = null;
        for (int i = start; i < start + length; i++) {
            if (rb == null) {
                rb = RoaringBitmap.bitmapOf(i);
            } else {
                rb.or(RoaringBitmap.bitmapOf(i));
            }
        }

        Slice slice = null;
        try {
            slice = Slices.wrappedBuffer(bitmapToBytes(rb));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return slice.toStringUtf8();
    }

    @Override
    public void testNoPositions() {
        super.testNoPositions();
    }

    @Override
    public void testSinglePosition() {

    }

    @Override
    public void testAllPositionsNull() {

    }

    @Override
    public void testMultiplePositions() {

    }

    @Override
    public void testMixedNullAndNonNullPositions() {

    }

    @Override
    public void testNegativeOnlyValues() {

    }

    @Override
    public void testPositiveOnlyValues() {
    }

    @Override
    public void testSlidingWindow() {


    }

    @Override
    protected String getFunctionName() {
        return "bitmap_union";
    }

    @Override
    protected List<Type> getFunctionParameterTypes() {
        return ImmutableList.of(VARBINARY);
    }


}
