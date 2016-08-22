package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuilt.Builder.class)
@DynamoDBTable(tableName = "simple_free_built")
interface SimpleFreeBuilt {
    String getHashKey();
    String getStringValue();

    class Builder extends SimpleFreeBuilt_Builder {}
}
