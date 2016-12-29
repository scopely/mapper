package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public final class JsonNodeAttributeValueMapper {
    public static Map<String, AttributeValue> convert(JsonNode node) throws MappingException {
        if (!node.isObject()) {
            throw new MappingException("Cannot convert non-object of type " + node.getNodeType());
        }
        return makeAVMapForObject(node);
    }

    public static ObjectNode convert(Map<String, AttributeValue> map, ObjectMapper objectMapper) throws MappingException {
        ObjectNode root = objectMapper.createObjectNode();

        for (Map.Entry<String, AttributeValue> entry : map.entrySet()) {
            AttributeValue attributeValue = entry.getValue();

            //noinspection PointlessBooleanExpression
            if (attributeValue.getNULL() != null) {
                root.putNull(entry.getKey());
            } else if (attributeValue.getBOOL() != null) {
                root.put(entry.getKey(), attributeValue.getBOOL());
            } else if (attributeValue.getS() != null) {
                root.put(entry.getKey(), attributeValue.getS());
            } else if (attributeValue.getN() != null) {
                // Since Dynamo also has non-interpreted numerals, this should work
                String numeric = attributeValue.getN();
                root.put(entry.getKey(), new BigDecimal(numeric));
            } else if (attributeValue.getM() != null) {
                ObjectNode childNode = root.putObject(entry.getKey());
                ObjectNode convertedMap = convert(attributeValue.getM(), objectMapper);
                childNode.setAll(convertedMap);
            } else if (attributeValue.getSS() != null) {
                List<String> ss = attributeValue.getSS();
                ArrayNode arrayNode = root.arrayNode();
                ss.forEach(arrayNode::add);
            } else if (attributeValue.getNS() != null) {
                List<String> ns = attributeValue.getNS();
                ArrayNode arrayNode = root.arrayNode();
                ns.forEach(n -> arrayNode.add(new BigDecimal(n)));
            } else {
                throw new MappingException(String.format("Couldn't interpret %s => %s", entry.getKey(), entry.getValue()));
            }
        }

        return root;
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
