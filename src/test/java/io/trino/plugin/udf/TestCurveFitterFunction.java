package io.trino.plugin.udf;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import org.testng.annotations.Test;
import static io.trino.spi.type.DoubleType.DOUBLE;


public class TestCurveFitterFunction
        extends AbstractTestUDFs
{
    @Test
    public void tesTestCurveFitter()
    {
        assertFunction("curve_fit(map(ARRAY[1.0, 2.0, 3.0], ARRAY[1.73, 2.82, 3.65]))", new ArrayType(DOUBLE),
                ImmutableList.of(0.5455733292266904, 3.5010680909887597, 0.5122800661447634, 0.8902872861117469));
    }
}
