package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.inferred.freebuilder.FreeBuilder;

import java.util.List;
import java.util.Map;

@FreeBuilder
@JsonDeserialize(builder = FreeBuiltWithLists.Builder.class)
@DynamoDBTable(tableName = "free_built_with_lists")
interface FreeBuiltWithLists {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();

    List<Double> getDoubleList();

    List<String> getStringList();

    List<Map<String, String>> getMapsList();

    class Builder extends FreeBuiltWithLists_Builder {
    }
}
