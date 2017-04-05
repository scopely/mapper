package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class JsonNodeAttributeValueMapperTest {
    @Test
    @Parameters(method = "conversions")
    public void convertToMap(String json, Map<String, Set<Map.Entry<String, AttributeValue>>> map) throws Exception {
        JsonNode node = new ObjectMapper().readTree(json);
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(node);
        assertThat(attributeValueMap).isEqualTo(map);
    }

    @Test
    public void convertBinaryToMap() throws Exception {
        ObjectNode node = new ObjectMapper().createObjectNode();
        String base64BinaryInfo = "12345=";
        node.put("key", ByteBuffer.wrap(base64BinaryInfo.getBytes("UTF-8")).array());
        AttributeValue attributeValue = new AttributeValue().withB(ByteBuffer.wrap(base64BinaryInfo.getBytes("UTF-8")));
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(node);
        assertThat(attributeValueMap.get("key")).isEqualTo(attributeValue);
    }

    @Test
    public void convertBinaryFromMap() throws Exception {
        String base64BinaryInfo = "12345=";
        Map<String, AttributeValue> map = ImmutableMap.of("key", new AttributeValue().withB(ByteBuffer.wrap(base64BinaryInfo.getBytes("UTF-8"))));
        ObjectNode node = JsonNodeAttributeValueMapper.convert(map, new ObjectMapper());
        assertThat(node.get("key").binaryValue()).isEqualTo(map.get("key").getB().array());
    }

    @Test(expected = MappingException.class)
    public void convert_array_mixedTypes_throws() throws Exception {
        JsonNode node = new ObjectMapper().readTree("{\"key\": [1,2,\"3\"]}");
        JsonNodeAttributeValueMapper.convert(node);
    }

    @Test(expected = MappingException.class)
    public void convert_array_booleans_throws() throws Exception {
        JsonNode node = new ObjectMapper().readTree("{\"key\": [false, true]}");
        JsonNodeAttributeValueMapper.convert(node);
    }

    @Test(expected = MappingException.class)
    @Parameters({"false", "1", "\"s\""})
    public void convert_requires_topLevel_object(String json) throws Exception {
        JsonNode node = new ObjectMapper().readTree(json);
        JsonNodeAttributeValueMapper.convert(node);
    }

    @SuppressWarnings("unused")
    private Object[] conversions() {
        return new Object[]
                {
                        new Object[] {"{\"key\": 1}", ImmutableMap.of("key", new AttributeValue().withN("1"))},
                        new Object[] {"{\"key\": \"1\"}", ImmutableMap.of("key", new AttributeValue().withS("1"))},
                        new Object[] {"{\"key\": {\"nested_key\": 1}, \"value\": 1}",
                                ImmutableMap.of(
                                        "key", new AttributeValue()
                                            .withM(ImmutableMap.of("nested_key",
                                                new AttributeValue().withN("1"))),
                                        "value", new AttributeValue().withN("1"))},
                        new Object[] {"{\"key\": {\"nested_key\": false}, \"value\": true}",
                                ImmutableMap.of(
                                        "key", new AttributeValue()
                                                .withM(ImmutableMap.of("nested_key",
                                                        new AttributeValue().withBOOL(false))),
                                        "value", new AttributeValue().withBOOL(true))},
                        new Object[] {"{\"key\": [1,2,3]}",
                                ImmutableMap.of("key", new AttributeValue().withNS("1", "2", "3"))},
                };
    }
}