package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.aws.dynamo.local.DynamoLocal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("ALL")
public class TablePersistenceTest {
    private DynamoLocal dynamoLocal;
    private AmazonDynamoDBClient amazonDynamoDBClient;
    private ObjectMapper objectMapper = new ObjectMapper();

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
        jsonDynamoMapper.putItem(objectMapper.readTree("{\"hk\": \"value\"}"), "hk_only", Collections.emptyList());
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

        jsonDynamoMapper.save(instance);

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

        jsonDynamoMapper.save(instance);

        SimpleFreeBuilt item = jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk").get();

        assertThat(item).isEqualToComparingFieldByField(instance);

        jsonDynamoMapper.delete(SimpleFreeBuilt.class, "hk");
        assertThat(jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk").isPresent()).isFalse();
    }

    @Test
    public void simple_free_built_persists_emtpy_string() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        SimpleFreeBuilt instance = new SimpleFreeBuilt.Builder().setHashKey("hk").setStringValue("").build();

        jsonDynamoMapper.save(instance);

        SimpleFreeBuilt item = jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk").get();

        assertThat(item.getStringValue()).isNull();

        SimpleFreeBuilt updatedInstance = new SimpleFreeBuilt.Builder().setHashKey("hk").setStringValue("val").build();
        jsonDynamoMapper.save(updatedInstance);

        SimpleFreeBuilt updatedItem = jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk").get();

        assertThat(updatedItem).isEqualToComparingFieldByField(updatedInstance);

        jsonDynamoMapper.delete(SimpleFreeBuilt.class, "hk");
        assertThat(jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk").isPresent()).isFalse();
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

        jsonDynamoMapper.save(instance);

        GetItemResult item = amazonDynamoDBClient.getItem("simple_free_built_versioned",
                ImmutableMap.of("hashKey", new AttributeValue().withS("hk")));

        assertThat(item.getItem()).containsEntry("version", new AttributeValue().withN("1"));

        jsonDynamoMapper.save(new SimpleFreeBuiltVersioned.Builder().setVersion(1).mergeFrom(instance).build());

        SimpleFreeBuiltVersioned retrieved = jsonDynamoMapper.load(SimpleFreeBuiltVersioned.class, "hk").get();

        assertThat(retrieved.getVersion()).hasValue(2);
    }

    @Test
    public void simple_free_built_innerDocument() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built_versioned");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        ImmutableList<InnerDocument> innerDocuments = ImmutableList.of(
                new InnerDocument.Builder().setRequiredInnerValue("value1").build(),
                new InnerDocument.Builder().setRequiredInnerValue("value2").setOptionalRequiredValue(42).build()
        );

        SimpleFreeBuiltVersioned instance =
                new SimpleFreeBuiltVersioned.Builder()
                        .setHashKey("hk")
                        .setStringValue("val")
                        .addAllInnerDocuments(innerDocuments)
                        .build();

        jsonDynamoMapper.save(instance);

        GetItemResult item = amazonDynamoDBClient.getItem("simple_free_built_versioned",
                ImmutableMap.of("hashKey", new AttributeValue().withS("hk")));

        assertThat(item.getItem()).containsEntry("version", new AttributeValue().withN("1"));

        jsonDynamoMapper.save(new SimpleFreeBuiltVersioned.Builder().setVersion(1).mergeFrom(instance).build());

        SimpleFreeBuiltVersioned retrieved = jsonDynamoMapper.load(SimpleFreeBuiltVersioned.class, "hk").get();

        assertThat(retrieved.getVersion()).hasValue(2);
    }

    @Test
    public void free_built_with_lists() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("free_built_with_lists");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        ImmutableList<InnerDocument> innerDocuments = ImmutableList.of(
                new InnerDocument.Builder().setRequiredInnerValue("value1").build(),
                new InnerDocument.Builder().setRequiredInnerValue("value2").setOptionalRequiredValue(42).build()
        );

        FreeBuiltWithLists instance =
                new FreeBuiltWithLists.Builder()
                        .setHashKey("hk")
                        .addAllDoubleList(ImmutableList.of(42.0, 52.0))
                        .addAllStringList(Collections.singletonList("fus-ro-dah"))
                        .build();

        FreeBuiltWithLists saved = jsonDynamoMapper.saveAndGet(instance);
        assertThat(saved).isEqualTo(instance);

        FreeBuiltWithLists retrieved = jsonDynamoMapper.load(FreeBuiltWithLists.class, "hk").get();

        assertThat(retrieved).isEqualTo(instance);
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

        jsonDynamoMapper.save(instance);

        jsonDynamoMapper.save(instance);
    }

    @Test
    public void load_missingItem_emptyReturn() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        Optional<SimpleFreeBuilt> loaded = jsonDynamoMapper.load(SimpleFreeBuilt.class, "hk");

        assertThat(loaded).isEmpty();
    }

    @Test
    public void load_hashAndRange_found() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey("even cooler").build();

        jsonDynamoMapper.save(har);

        Optional<HashAndRange> found = jsonDynamoMapper.load(HashAndRange.class, "yo this is cool", "even cooler");

        assertThat(found).hasValue(har);

        jsonDynamoMapper.delete(HashAndRange.class, "yo this is cool", "even cooler");
        assertThat(jsonDynamoMapper.load(HashAndRange.class, "yo this is cool", "even cooler").isPresent()).isFalse();
    }

    @Test
    public void load_hashAndRangeAutoGenerated_found() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey(null).build();

        PutItemResult savedItem = jsonDynamoMapper.save(har);
        AttributeValue rangeKey = savedItem.getAttributes().get("rangeKey");

        assertThat(rangeKey).isNotNull();
        assertThat(rangeKey.getS()).isNotNull();

        Optional<HashAndRange> found = jsonDynamoMapper.load(HashAndRange.class, "yo this is cool", rangeKey.getS());

        assertThat(found).hasValue(new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey(rangeKey.getS()).build());
    }

    @Test
    public void load_hashAndOptionalRangeAutoGenerated_found() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        HashAndOptionalRange har = new HashAndOptionalRange.Builder().setHashKey("yo this is cool").build();

        PutItemResult savedItem = jsonDynamoMapper.save(har);
        AttributeValue rangeKey = savedItem.getAttributes().get("rangeKey");

        assertThat(rangeKey).isNotNull();
        assertThat(rangeKey.getS()).isNotNull();

        Optional<HashAndOptionalRange> found = jsonDynamoMapper.load(HashAndOptionalRange.class, "yo this is cool", rangeKey.getS());

        assertThat(found).hasValue(new HashAndOptionalRange.Builder().setHashKey("yo this is cool").setRangeKey(rangeKey.getS()).build());
    }

    @Test(expected = MappingException.class)
    public void invalidAutoGeneratedOptionalType() throws Exception {
        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);
        InvalidRange har = new InvalidRange.Builder().setHashKey("yo this is cool").build();
        jsonDynamoMapper.save(har);
    }

    @Test
    public void load_hashAndRange_notFound() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);
        Optional<HashAndRange> found = jsonDynamoMapper.load(HashAndRange.class, "yo this is cool", "even cooler");
        assertThat(found).isEmpty();
    }

    @Test
    public void scan_pagination() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        for (int i = 0; i < 1000; i++) {
            HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey("" + i).build();
            jsonDynamoMapper.save(har);
        }

        List<HashAndRange> results = new ArrayList<>();

        ScanResultPage<HashAndRange> scan = jsonDynamoMapper.scan(HashAndRange.class,
                new DynamoDBScanExpression().withLimit(100));
        results.addAll(scan.getResults());
        assertThat(scan.getCount()).isGreaterThan(10);
        assertThat(scan.getCount()).isLessThan(1000);
        assertThat(scan.getLastEvaluatedKey()).isNotNull();
        while (scan.getLastEvaluatedKey() != null) {
            scan = jsonDynamoMapper.scan(HashAndRange.class,
                    new DynamoDBScanExpression().withExclusiveStartKey(scan.getLastEvaluatedKey()));
            results.addAll(scan.getResults());
        }

        assertThat(results).hasSize(1000);
    }

    @Test
    public void scan_all() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        for (int i = 0; i < 1000; i++) {
            HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey("" + i).build();
            jsonDynamoMapper.save(har);
        }

        List<HashAndRange> scan = jsonDynamoMapper.scanAll(HashAndRange.class);
        assertThat(scan).hasSize(1000);
    }

    @Test
    public void query_pagination() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(
                    new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        for (int i = 0; i < 1000; i++) {
            HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey("" + i).build();
            jsonDynamoMapper.save(har);
        }

        List<HashAndRange> results = new ArrayList<>();

        QueryResultPage<HashAndRange> query = jsonDynamoMapper
                .query(HashAndRange.class,
                       new DynamoDBQueryExpression()
                               .withKeyConditionExpression("hashKey = :hashKey")
                               .withExpressionAttributeValues(ImmutableMap.of(":hashKey", new AttributeValue("yo this is cool")))
                               .withLimit(100));
        results.addAll(query.getResults());
        assertThat(query.getCount()).isGreaterThan(10);
        assertThat(query.getCount()).isLessThan(1000);
        assertThat(query.getLastEvaluatedKey()).isNotNull();
        while (query.getLastEvaluatedKey() != null) {
            query = jsonDynamoMapper.query(
                    HashAndRange.class,
                    new DynamoDBQueryExpression()
                            .withKeyConditionExpression("hashKey = :hashKey")
                            .withExpressionAttributeValues(ImmutableMap.of(":hashKey", new AttributeValue("yo this is cool")))
                            .withExclusiveStartKey(query.getLastEvaluatedKey()));
            results.addAll(query.getResults());
        }

        assertThat(results).hasSize(1000);
    }

    @Test
    public void query_all() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(
                    new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);

        for (int i = 0; i < 1000; i++) {
            HashAndRange har = new HashAndRange.Builder().setHashKey("yo this is cool").setRangeKey("" + i).build();
            jsonDynamoMapper.save(har);
        }

        List<HashAndRange> results = new ArrayList<>();

        List<HashAndRange> query = jsonDynamoMapper
                .queryAll(HashAndRange.class,
                       new DynamoDBQueryExpression()
                               .withKeyConditionExpression("hashKey = :hashKey")
                               .withExpressionAttributeValues(ImmutableMap.of(":hashKey", new AttributeValue("yo this is cool"))));
        assertThat(query).hasSize(1000);
    }

    @Test
    public void simple_free_built_binary_persists() throws Exception {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("simple_free_built_with_binary");
            ctr.setKeySchema(ImmutableList.of(new KeySchemaElement("hashKey", KeyType.HASH)));
            ctr.setAttributeDefinitions(ImmutableList.of(new AttributeDefinition("hashKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);
        ByteBuffer bb = ByteBuffer.wrap("val".getBytes());
        SimpleFreeBuiltWithBinaryAttribute instance = new SimpleFreeBuiltWithBinaryAttribute.Builder()
                                                    .setHashKey("hk").setByteBufferValue(bb).build();

        jsonDynamoMapper.save(instance);

        SimpleFreeBuiltWithBinaryAttribute item = jsonDynamoMapper.load(SimpleFreeBuiltWithBinaryAttribute.class, "hk").get();

        assertThat(item).isEqualToComparingFieldByField(instance);

        jsonDynamoMapper.delete(SimpleFreeBuiltWithBinaryAttribute.class, "hk");
        assertThat(jsonDynamoMapper.load(SimpleFreeBuiltWithBinaryAttribute.class, "hk").isPresent()).isFalse();
    }

    @Test
    public void save_all() throws JsonProcessingException {
        dynamoLocal.createTable(ctr -> {
            ctr.setTableName("hash_and_range");
            ctr.setKeySchema(ImmutableList.of(
                    new KeySchemaElement("hashKey", KeyType.HASH),
                    new KeySchemaElement("rangeKey", KeyType.RANGE)));
            ctr.setAttributeDefinitions(ImmutableList.of(
                    new AttributeDefinition("hashKey", ScalarAttributeType.S),
                    new AttributeDefinition("rangeKey", ScalarAttributeType.S)));
        });

        JsonDynamoMapper jsonDynamoMapper = new JsonDynamoMapper(amazonDynamoDBClient);
        List<HashAndRange> items = new ArrayList<>();
        items.add(new HashAndRange.Builder().setHashKey("hk1").build());
        items.add(new HashAndRange.Builder().setHashKey("hk2").build());

        jsonDynamoMapper.saveAll(HashAndRange.class, items);

        List<HashAndRange> dbItems = jsonDynamoMapper.scanAll(HashAndRange.class);
        for (HashAndRange hr : dbItems) {
            HashAndRange item = dbItems.stream().filter(i -> i.getHashKey() == hr.getHashKey()).findFirst().get();
            assertThat(item).isEqualToComparingFieldByField(hr);

            jsonDynamoMapper.delete(HashAndRange.class, hr.getHashKey(), hr.getRangeKey());
            assertThat(jsonDynamoMapper.load(HashAndRange.class, hr.getHashKey(), hr.getRangeKey()).isPresent()).isFalse();
        }
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
