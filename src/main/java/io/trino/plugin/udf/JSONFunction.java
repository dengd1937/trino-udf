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

import com.alibaba.fastjson.*;
import com.jayway.jsonpath.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * json of udf
 *
 * @author di_deng@droidhang.com
 */
public class JSONFunction
{
    @ScalarFunction(value = "get_json_object")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getJSONObject(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        requireNonNull(json, "json is null");
        requireNonNull(jsonPath, "jsonPath is null");

        String r;
        try {
            Object extract = JsonPath.read(json.toStringUtf8(), jsonPath.toStringUtf8());
            if (extract instanceof List || extract instanceof Map) {
                r = JSON.toJSONString(extract);
            }
            else if (extract instanceof Number) {
                r = String.valueOf(extract);
            }
            else if (extract instanceof Boolean) {
                r = String.valueOf(extract);
            }
            else {
                r = (String) extract;
            }
            return r == null ? null: Slices.utf8Slice(r);
        }
        catch (InvalidJsonException | InvalidPathException e) {
            // JSON字符串转换问题或者JsonPath错误
            return null;
        }

    }


}
