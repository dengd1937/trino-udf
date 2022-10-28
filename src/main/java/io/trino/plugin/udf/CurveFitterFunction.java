package io.trino.plugin.udf;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.fitting.SimpleCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import static io.trino.spi.type.DoubleType.DOUBLE;

/**
 * 专用做于计算LTV
 *
 * @author yucheng
 */
public class CurveFitterFunction
{
    @ScalarFunction(value = "curve_fit", deterministic = true)
    @Description("曲线拟合")
    @SqlType("array(double)")
    public static Block curveFitter(@SqlType("map(double,double)") Block block)
    {
        ParametricUnivariateFunction function = new MyFunction();/*曲线拟合*/
        double[] guess = {1, 1, 1, 1}; /*初始值 依次为 a b c d 。必须和 gradient 方法返回数组对应。如果不知道都设置为 1*/
        // 初始化拟合
        SimpleCurveFitter curveFitter = SimpleCurveFitter.create(function, guess);
        // 添加数据点
        WeightedObservedPoints observedPoints = new WeightedObservedPoints();
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            double key = DOUBLE.getDouble(block, i);
            double value = DOUBLE.getDouble(block, i + 1);
            observedPoints.add(key, value);
        }
        double[] best = curveFitter.fit(observedPoints.toList());

        BlockBuilder output = DOUBLE.createBlockBuilder(null, 4);
        for (int i = 0; i <= 3; i++) {
            DOUBLE.writeDouble(output, best[i]);
        }
        return output.build();
    }

    static class MyFunction implements ParametricUnivariateFunction
    {
        public double value(double x, double... parameters)
        {
            double a = parameters[0];
            double b = parameters[1];
            double c = parameters[2];
            double d = parameters[3];
            return a + b * Math.log(c * x + d);
        }

        public double[] gradient(double x, double... parameters)
        {
            double a = parameters[0];
            double b = parameters[1];
            double c = parameters[2];
            double d = parameters[3];
            double[] gradients = new double[4];
            gradients[0] = 1; // 对 a 求导
            gradients[1] = Math.log(c * x + d); // 对 b 求导
            gradients[2] = (b * x) / (c * x + d); // 对 c 求导
            gradients[3] = b / (c * x + d); // 对 d 求导
            return gradients;
        }
    }
}
