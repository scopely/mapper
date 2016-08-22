package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.aws.dynamo.local.DynamoLocal;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;

public class TablePersistenceTest {
    DynamoLocal dynamoLocal;
    AmazonDynamoDBClient amazonDynamoDBClient;
    ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        dynamoLocal = new DynamoLocal();
        dynamoLocal.start();
        amazonDynamoDBClient = dynamoLocal.buildDynamoClient();
    }
    
    @After
    public void tearDown() throws Exception {
        dynamoLocal.stop();
    }

    @Test
    public void hashKey_only() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hk_only");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hk", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hk", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);
        jsonDynamoMapper.putItem(objectMapper.readTree("{\"hk\": \"value\"}"), "hk_only");
    }

    @Test
    public void simple_annotated_class() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hk_class_annotated");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("s", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("s", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        SimpleAnnotatedClass instance = new SimpleAnnotatedClass("key", false);

        jsonDynamoMapper.putItem(SimpleAnnotatedClass.class, instance);

        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDBClient);

        SimpleAnnotatedClass restored = mapper.load(SimpleAnnotatedClass.class, "key");

        assertThat(restored).isEqualToComparingOnlyGivenFields(instance, "s", "b");
        assertThat(restored.getV()).isEqualTo(1);
    }

    @Test
    public void simple_free_built_persists() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        SimpleFreeBuilt instance = new SimpleFreeBuilt.Builder().setHashKey("hk").setStringValue("val").build();

        jsonDynamoMapper.putItem(SimpleFreeBuilt.class, instance);
    }

    @Test
    public void simple_free_built_versionIncrements() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built_versioned");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        SimpleFreeBuiltVersioned instance =
                new SimpleFreeBuiltVersioned.Builder()
                        .setHashKey("hk")
                        .setStringValue("val")
                        .build();

        jsonDynamoMapper.putItem(SimpleFreeBuiltVersioned.class, instance);

        GetItemResult item = amazonDynamoDBClient.getItem("simple_free_built_versioned",
                ImmutableMap.of("hashKey", new AttributeValue().withS("hk")));

        assertThat(item.getItem()).containsEntry("version", new AttributeValue().withN("1"));

        jsonDynamoMapper.putItem(SimpleFreeBuiltVersioned.class,
                new SimpleFreeBuiltVersioned.Builder().setVersion(1).mergeFrom(instance).build());

        item = amazonDynamoDBClient.getItem("simple_free_built_versioned",
                ImmutableMap.of("hashKey", new AttributeValue().withS("hk")));

        assertThat(item.getItem()).containsEntry("version", new AttributeValue().withN("2"));
    }

    @Test(expected = ConditionalCheckFailedException.class)
    public void simple_free_built_versionChecked() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built_versioned");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        SimpleFreeBuiltVersioned instance =
                new SimpleFreeBuiltVersioned.Builder()
                        .setHashKey("hk")
                        .setStringValue("val")
                        .build();

        jsonDynamoMapper.putItem(SimpleFreeBuiltVersioned.class, instance);

        jsonDynamoMapper.putItem(SimpleFreeBuiltVersioned.class, instance);
    }

    /**
     * Example class with standard annotations; still supported.
     */
    @DynamoDBTable(tableName = "hk_class_annotated")
    public static class SimpleAnnotatedClass {
        private String s;
        private boolean b;
        @Nullable private Integer v;

        public SimpleAnnotatedClass() {
        }

        public SimpleAnnotatedClass(String s, boolean b) {
            this.s = s;
            this.b = b;
        }

        @DynamoDBHashKey
        public String getS() {
            return s;
        }

        public boolean isB() {
            return b;
        }

        @DynamoDBVersionAttribute(attributeName = "v")
        public Integer getV() {
            return v;
        }

        public void setS(String s) {
            this.s = s;
        }

        public void setB(boolean b) {
            this.b = b;
        }

        public void setV(@Nullable Integer v) {
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimpleAnnotatedClass simpleAnnotatedClass = (SimpleAnnotatedClass) o;

            if (b != simpleAnnotatedClass.b) return false;
            if (s != null ? !s.equals(simpleAnnotatedClass.s) : simpleAnnotatedClass.s != null) return false;
            return v != null ? v.equals(simpleAnnotatedClass.v) : simpleAnnotatedClass.v == null;

        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + (b ? 1 : 0);
            result = 31 * result + (v != null ? v.hashCode() : 0);
            return result;
        }
    }

}
