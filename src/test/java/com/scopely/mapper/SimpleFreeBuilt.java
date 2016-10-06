package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import javax.annotation.Nullable;

@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuilt.Builder.class)
@DynamoDBTable(tableName = "simple_free_built")
interface SimpleFreeBuilt {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();
    @Nullable
    String getStringValue();

    class Builder extends SimpleFreeBuilt_Builder {}
}
