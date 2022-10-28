package io.trino.plugin.udf;


import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestJSONFunction
        extends AbstractTestUDFs
{
    @Test
    public void tesGetJSONObject()
    {
        assertFunction("get_json_object('{\"a\": true}', '$.a')", VARCHAR, "true");
        assertFunction("get_json_object('{\"a\":{\\\"b\\\":\\\"phone\\\"}}', '$.a.\\\"b\\\"')", VARCHAR, "\\\"phone\\\"");

        assertFunction("get_json_object('{\"a\":\"b\"}', '$.a')", VARCHAR, "b");
        assertFunction("get_json_object('{\"a\":\"b\"}', '$.c')", VARCHAR, null);
        assertFunction("get_json_object('{\"a\":\"邓荻\"}', '$.a')", VARCHAR, "邓荻");
        assertFunction("get_json_object('{\"a\":\"b}', '$.a')", VARCHAR, null);

        assertFunction("get_json_object('{\"1\": {}}', '$.1')", VARCHAR, "{}");
        assertFunction("get_json_object('{\"_\": 1}', '$._')", VARCHAR, "1");
        assertFunction("get_json_object('{\"1.1\": \"A\"}', '$.1.1')", VARCHAR, null);

        assertFunction("get_json_object('{\"a\":{\"b\":1,\"c\":2}}', '$.a')", VARCHAR, "{\"b\":1,\"c\":2}");
        assertFunction("get_json_object('{\"1\":[\"A\",\"B\",\"C\"]}', '$.1')", VARCHAR, "[\"A\",\"B\",\"C\"]");
        assertFunction("get_json_object('{\"1\":[\"A\",\"B\",\"C\"]}', '$.1[0]')", VARCHAR, "A");

        assertFunction("get_json_object(get_json_object('{\"a\":{\"b\":1,\"c\":2}}', '$.a'), '$.b')", VARCHAR, "1");
        assertFunction("get_json_object('{\"a\":{\"b\":1,\"c\":2}}', '$.a.b')", VARCHAR, "1");
        assertFunction("get_json_object('{\"a\":{\"b\":[1,2,3],\"c\":2}}', '$.a.b')", VARCHAR, "[1,2,3]");
        assertFunction("get_json_object('{\"a\":{\"b\":[1,2,3],\"c\":2}}', '$.a.b[0]')", VARCHAR, "1");


        assertFunction("get_json_object('{}', '$')", VARCHAR, "{}");
        assertFunction("get_json_object('{\"fuu\": {\"bar\": 1}}', '$.fuu')", VARCHAR, "{\"bar\":1}");
        assertFunction("get_json_object('{\"fuu\": 1}', '$.fuu')", VARCHAR, "1");
        assertFunction("get_json_object('{\"fuu\": 1}', '$[fuu]')", VARCHAR, null);
        assertFunction("get_json_object('{\"fuu\": 1}', '$[\"fuu\"]')", VARCHAR, "1");
        assertFunction("get_json_object('{\"fuu\": null}', '$.fuu')", VARCHAR, null);
        assertFunction("get_json_object('{\"fuu\": 1}', '$.bar')", VARCHAR, null);
        assertFunction("get_json_object('{\"fuu\": 1, \"bar\": \"abc\"}', '$.bar')", VARCHAR,"abc");
        assertFunction("get_json_object('{\"fuu\": [0.1, 1, 2]}', '$.fuu[0]')", VARCHAR, "0.1");
        assertFunction("get_json_object('{\"fuu\": [0, [100, 101], 2]}', '$.fuu[1]')", VARCHAR, "[100,101]");
        assertFunction("get_json_object('{\"fuu\": [0, [100, 101], 2]}', '$.fuu[1][1]')", VARCHAR, "101");

        // Test non-object extraction
        assertFunction("get_json_object('[0, 1, 2]', '$[0]')", VARCHAR, "0");
        assertFunction("get_json_object('\"abc\"', '$')", VARCHAR,"abc");
        assertFunction("get_json_object('123', '$')", VARCHAR,"123");
        assertFunction("get_json_object(null, '$')", VARCHAR,null);

        // Test extraction using bracket json path
        assertFunction("get_json_object('{\"fuu\": {\"bar\": 1}}', '$[\"fuu\"]')", VARCHAR, "{\"bar\":1}");
        assertFunction("get_json_object('{\"fuu\": {\"bar\": 1}}', '$[\"fuu\"][\"bar\"]')", VARCHAR,"1");
        assertFunction("get_json_object('{\"fuu\": 1}', '$[\"fuu\"]')", VARCHAR, "1");
        assertFunction("get_json_object('{\"fuu\": null}', '$[\"fuu\"]')", VARCHAR, null);
        assertFunction("get_json_object('{\"fuu\": 1}', '$[\"bar\"]')", VARCHAR, null);
        assertFunction("get_json_object('{\"fuu\": 1, \"bar\": \"abc\"}', '$[\"bar\"]')", VARCHAR, "abc");
        assertFunction("get_json_object('{\"fuu\": [0.1, 1, 2]}', '$[\"fuu\"][0]')", VARCHAR, "0.1");
        assertFunction("get_json_object('{\"fuu\": [0, [100, 101], 2]}', '$[\"fuu\"][1]')", VARCHAR, "[100,101]");
        assertFunction("get_json_object('{\"fuu\": [0, [100, 101], 2]}', '$[\"fuu\"][1][1]')", VARCHAR, "101");

        // Test extraction using bracket json path with special json characters in path
        assertFunction("get_json_object('{\"@$fuu\": {\".b.ar\": 1}}', '$[\"@$fuu\"]')", VARCHAR, "{\".b.ar\":1}");
        assertFunction("get_json_object('{\"fuu..\": 1}', '$[\"fuu..\"]')", VARCHAR, "1");
        assertFunction("get_json_object('{\"fu*u\": null}', '$[\"fu*u\"]')", VARCHAR, null);
        assertFunction("get_json_object('{\",fuu\": 1}', '$[\"bar\"]')", VARCHAR, null);
        assertFunction("get_json_object('{\":fu:u:\": 1, \":b:ar:\": \"abc\"}', '$[\":b:ar:\"]')", VARCHAR, "abc");
        assertFunction("get_json_object('{\"?()fuu\": [0.1, 1, 2]}', '$[\"?()fuu\"][0]')", VARCHAR, "0.1");
        assertFunction("get_json_object('{\"f?uu\": [0, [100, 101], 2]}', '$[\"f?uu\"][1]')", VARCHAR, "[100,101]");
        assertFunction("get_json_object('{\"fuu()\": [0, [100, 101], 2]}', '$[\"fuu()\"][1][1]')", VARCHAR, "101");

        // Test extraction using mix of bracket and dot notation json path
        assertFunction("get_json_object('{\"fuu\": {\"bar\": 1}}', '$[\"fuu\"].bar')", VARCHAR, "1");
        assertFunction("get_json_object('{\"fuu\": {\"bar\": 1}}', '$.fuu[\"bar\"]')", VARCHAR, "1");

        // Test extraction using  mix of bracket and dot notation json path with special json characters in path
        assertFunction("get_json_object('{\"@$fuu\": {\"bar\": 1}}', '$[\"@$fuu\"].bar')", VARCHAR, "1");

        // Test numeric path expression matches arrays and objects
        assertFunction("get_json_object('[0, 1, 2]', '$.1')", VARCHAR,null);
        assertFunction("get_json_object('[0, 1, 2]', '$[1]')", VARCHAR,"1");
        assertFunction("get_json_object('[0, 1, 2]', '$[\"1\"]')", VARCHAR, null);
        assertFunction("get_json_object('{\"0\" : 0, \"1\" : 1, \"2\" : 2, }', '$.1')", VARCHAR, "1");
        assertFunction("get_json_object('{\"0\" : 0, \"1\" : 1, \"2\" : 2, }', '$[1]')", VARCHAR,null);
        assertFunction("get_json_object('{\"0\" : 0, \"1\" : 1, \"2\" : 2, }', '$[\"1\"]')", VARCHAR,"1");

        // Test fields starting with a digit
        assertFunction("get_json_object('{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }', '$.30day')", VARCHAR, "1");
        assertFunction("get_json_object('{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }', '$[30day]')", VARCHAR,null);
        assertFunction("get_json_object('{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }', '$[\"30day\"]')", VARCHAR,"1");
    }
}
