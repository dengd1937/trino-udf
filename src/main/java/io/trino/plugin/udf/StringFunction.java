/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.udf;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * string of udf
 *
 * @author di_deng@droidhang.com
 */
public class StringFunction
{
    @ScalarFunction(value = "is_blank", deterministic = true)
    @Description("Returns TRUE if the argument is whitespace, empty or null")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isBlank(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        if (slice == null) {
            return true;
        }
        String argument = slice.toStringUtf8();
        int strLen = argument.length();
        if (strLen == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(argument.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
