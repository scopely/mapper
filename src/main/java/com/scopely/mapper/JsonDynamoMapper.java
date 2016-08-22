package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.sun.istack.internal.NotNull;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Map;

public class JsonDynamoMapper {
    private AmazonDynamoDB amazonDynamoDB;
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public <T> PutItemResult putItem(Class<T> clazz, T item) throws MappingException {
        JsonNode serialized = objectMapper.valueToTree(item);
        @Nullable DynamoDBTable tableAnnotation = clazz.getAnnotation(DynamoDBTable.class);
        if (tableAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBTable.class);
        }

        Method[] methods = clazz.getMethods();
        DynamoDBVersionAttribute versionAnnotation = null;
        for (Method method : methods) {
            if ((versionAnnotation = method.getAnnotation(DynamoDBVersionAttribute.class)) != null) {
                break;
            }
        }

        if (versionAnnotation != null) {
            return putItem(serialized, tableAnnotation.tableName(), versionAnnotation.attributeName());
        }

        return putItem(serialized, tableAnnotation.tableName());
    }

    public PutItemResult putItem(JsonNode jsonNode, String table) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);
        return amazonDynamoDB.putItem(new PutItemRequest().withTableName(table).withItem(attributeValueMap));
    }

    public PutItemResult putItem(JsonNode jsonNode, String table, @NotNull String versionField) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);

        @Nullable AttributeValue currentVersion = attributeValueMap.get(versionField);

        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(table)
                .withExpressionAttributeNames(ImmutableMap.of("#v", versionField));

        if (currentVersion == null || currentVersion.getN() == null) {
            putItemRequest = putItemRequest.withConditionExpression("attribute_not_exists(#v)");
            ImmutableMap.Builder<String, AttributeValue> builder = new ImmutableMap.Builder<>();

            attributeValueMap.entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equals(versionField))
                    .forEach(e -> builder.put(e.getKey(), e.getValue()));

            builder.put(versionField, new AttributeValue().withN("1"));

            attributeValueMap = builder.build();
        } else {
            putItemRequest = putItemRequest.withConditionExpression("#v = :vf")
                    .withExpressionAttributeValues(ImmutableMap.of(":vf", new AttributeValue().withN(currentVersion.getN())));
            int v = Integer.parseInt(currentVersion.getN());
            currentVersion.setN(String.valueOf(v + 1));
        }

        return amazonDynamoDB.putItem(putItemRequest.withItem(attributeValueMap));
    }
}
