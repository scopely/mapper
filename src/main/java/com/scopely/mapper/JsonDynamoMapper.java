package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("WeakerAccess")
public class JsonDynamoMapper {
    private AmazonDynamoDB amazonDynamoDB;
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public <T> PutItemResult save(T item) throws MappingException {
        JsonNode serialized = objectMapper.valueToTree(item);

        Class<?> clazz = item.getClass();

        String tableName = tableName(clazz);

        Method[] methods = clazz.getMethods();
        DynamoDBVersionAttribute versionAnnotation = null;
        for (Method method : methods) {
            if ((versionAnnotation = method.getAnnotation(DynamoDBVersionAttribute.class)) != null) {
                break;
            }
        }

        if (versionAnnotation != null) {
            return putItem(serialized, tableName, versionAnnotation.attributeName());
        }

        return putItem(serialized, tableName);
    }

    public PutItemResult putItem(JsonNode jsonNode, String table) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);
        return amazonDynamoDB.putItem(new PutItemRequest().withTableName(table).withItem(attributeValueMap));
    }

    public PutItemResult putItem(JsonNode jsonNode, String table, @Nonnull String versionField) throws MappingException {
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

    public <T> Optional<T> load(Class<T> clazz, String hashKey) throws MappingException {
        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(hashKeyAttribute(clazz), new AttributeValue().withS(hashKey)));

        if (item.getItem() == null) {
            return Optional.empty();
        }

        ObjectNode converted = JsonNodeAttributeValueMapper.convert(item.getItem(), objectMapper);

        try {
            return Optional.of(objectMapper.readValue(converted.traverse(), clazz));
        } catch (IOException e) {
            throw new MappingException("Exception deserializing", e);
        }
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey, String rangeKey) throws MappingException {
        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(
                        hashKeyAttribute(clazz), new AttributeValue().withS(hashKey),
                        rangeKeyAttribute(clazz), new AttributeValue().withS(rangeKey)));

        if (item.getItem() == null) {
            return Optional.empty();
        }

        ObjectNode converted = JsonNodeAttributeValueMapper.convert(item.getItem(), objectMapper);

        try {
            return Optional.of(objectMapper.readValue(converted.traverse(), clazz));
        } catch (IOException e) {
            throw new MappingException("Exception deserializing", e);
        }
    }

    private <T> String tableName(Class<T> clazz) throws MappingException {
        @Nullable DynamoDBTable tableAnnotation = clazz.getAnnotation(DynamoDBTable.class);
        if (tableAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBTable.class);
        }

        return tableAnnotation.tableName();
    }

    private <T> String hashKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBHashKey hashKeyAnnotation = null;
        for (Method method : methods) {
            if ((hashKeyAnnotation = method.getAnnotation(DynamoDBHashKey.class)) != null) {
                break;
            }
        }

        if (hashKeyAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBHashKey.class);
        }

        return hashKeyAnnotation.attributeName();
    }

    private <T> String rangeKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBRangeKey rangeKeyAnnotation = null;
        for (Method method : methods) {
            if ((rangeKeyAnnotation = method.getAnnotation(DynamoDBRangeKey.class)) != null) {
                break;
            }
        }

        if (rangeKeyAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBRangeKey.class);
        }

        return rangeKeyAnnotation.attributeName();
    }
}
