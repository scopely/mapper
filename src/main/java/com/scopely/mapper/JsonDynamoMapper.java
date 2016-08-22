package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;

public class JsonDynamoMapper {
    private AmazonDynamoDB amazonDynamoDB;

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public PutItemResult putItem(JsonNode jsonNode, String table) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);
        return amazonDynamoDB.putItem(new PutItemRequest().withTableName(table).withItem(attributeValueMap));
    }

    public PutItemResult putItem(JsonNode jsonNode, String table, String versionField) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);

        @Nullable AttributeValue currentVersion = attributeValueMap.get(versionField);

        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(table)
                .withExpressionAttributeNames(ImmutableMap.of("#v", versionField));

        if (currentVersion == null) {
            putItemRequest = putItemRequest.withConditionExpression("attribute_not_exists(#v)");
            attributeValueMap = new ImmutableMap.Builder<String, AttributeValue>()
                    .putAll(attributeValueMap)
                    .put(versionField, new AttributeValue(versionField).withN("1"))
                    .build();
        } else {
            putItemRequest = putItemRequest.withConditionExpression("#v = :vf")
                    .withExpressionAttributeValues(ImmutableMap.of(":vf", new AttributeValue().withN(currentVersion.getN())));
            int v = Integer.parseInt(currentVersion.getN());
            currentVersion.setN(String.valueOf(v + 1));
        }

        return amazonDynamoDB.putItem(putItemRequest.withItem(attributeValueMap));
    }
}
