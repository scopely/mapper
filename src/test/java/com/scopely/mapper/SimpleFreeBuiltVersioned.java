package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuiltVersioned.Builder.class)
@DynamoDBTable(tableName = "simple_free_built_versioned")
interface SimpleFreeBuiltVersioned {
    String getHashKey();
    String getStringValue();
    @DynamoDBVersionAttribute(attributeName = "version")
    Optional<Integer> getVersion();

    class Builder extends SimpleFreeBuiltVersioned_Builder {}
}
