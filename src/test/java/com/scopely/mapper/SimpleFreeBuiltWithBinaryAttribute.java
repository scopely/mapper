package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.nio.ByteBuffer;

@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuiltWithBinaryAttribute.Builder.class)
@DynamoDBTable(tableName = "simple_free_built_with_binary")
public interface SimpleFreeBuiltWithBinaryAttribute {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();

    ByteBuffer getByteBufferValue();

    class Builder extends SimpleFreeBuiltWithBinaryAttribute_Builder {}

}
