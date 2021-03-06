package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.inferred.freebuilder.FreeBuilder;
import org.jetbrains.annotations.Nullable;

@FreeBuilder
@JsonDeserialize(builder = HashAndRange.Builder.class)
@DynamoDBTable(tableName = "hash_and_range")
interface HashAndRange {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();
    @Nullable
    @DynamoDBAutoGeneratedKey
    @DynamoDBRangeKey(attributeName = "rangeKey")
    String getRangeKey();

    class Builder extends HashAndRange_Builder {}
}
