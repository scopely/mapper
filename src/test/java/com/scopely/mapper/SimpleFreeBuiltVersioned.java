package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.inferred.freebuilder.FreeBuilder;

import java.util.List;
import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuiltVersioned.Builder.class)
@DynamoDBTable(tableName = "simple_free_built_versioned")
interface SimpleFreeBuiltVersioned {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();
    String getStringValue();
    @DynamoDBVersionAttribute(attributeName = "version")
    Optional<Integer> getVersion();

    List<InnerDocument> getInnerDocuments();

    class Builder extends SimpleFreeBuiltVersioned_Builder {}
}
