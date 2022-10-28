package io.trino.plugin.udf;

import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;

import static io.trino.metadata.FunctionExtractor.extractFunctions;

public class AbstractTestUDFs
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        functionAssertions.getMetadata().addFunctions(
                extractFunctions(new UDFPlugin().getFunctions()));
    }
}
