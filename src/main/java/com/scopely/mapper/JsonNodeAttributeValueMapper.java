package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class JsonNodeAttributeValueMapper {
    public static Map<String, AttributeValue> convert(JsonNode node) throws MappingException {
        if (!node.isObject()) {
            throw new MappingException("Cannot convert non-object of type " + node.getNodeType());
        }
        return makeAVMapForObject(node);
    }

    private static AttributeValue makeAV(JsonNode node) throws MappingException {
        JsonNodeType nodeType = node.getNodeType();

        AttributeValue attributeValue = new AttributeValue();

        switch (nodeType) {
            case NULL:
                attributeValue.setNULL(true);
                return attributeValue;
            case BOOLEAN:
                attributeValue.setBOOL(node.asBoolean());
                return attributeValue;
            case STRING:
                attributeValue.setS(node.asText());
                return attributeValue;
            case NUMBER:
                attributeValue.setN(node.asText());
                return attributeValue;
            case OBJECT:
            case POJO:    // TODO verify we handle one of these correctly
                attributeValue.setM(makeAVMapForObject(node));
                return attributeValue;
            case ARRAY:
                setAVForArray(node, attributeValue);
                return attributeValue;
            case MISSING:
            case BINARY:
            default:
                throw new MappingException("Unsupported exception " + nodeType);
        }
    }

    private static AttributeValue setAVForArray(JsonNode node, AttributeValue value) throws MappingException {
        ImmutableList<JsonNode> jsonNodes = ImmutableList.copyOf(node.elements());

        Set<JsonNodeType> types = jsonNodes.stream().map(JsonNode::getNodeType).collect(Collectors.toSet());

        if (types.size() > 1) {
            throw new MappingException("Mismatched types: " + types);
        }

        JsonNodeType type = types.iterator().next();

        switch (type) {
            case STRING:
                value.setSS(jsonNodes.stream().map(JsonNode::asText).collect(Collectors.toList()));
                return value;
            case NUMBER:
                value.setNS(jsonNodes.stream().map(JsonNode::asText).collect(Collectors.toList()));
                return value;
            default:
                throw new MappingException("Unsupported list type " + type);
        }
    }

    private static Map<String, AttributeValue> makeAVMapForObject(JsonNode node) throws MappingException {
        ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            builder.put(entry.getKey(), makeAV(entry.getValue()));
        }

        return builder.build();
    }
}
