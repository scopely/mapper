package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JsonDynamoMapper requires that all classes used with it can be safely round-tripped to JSON. In all cases, it
 * first converts to JSON, then to the target class or to a DynamoDB record. When creating a DynamoDB record, nested
 * objects will be converted into Map fields.
 *
 * There are some limitations to the mapping-- JSON supports arrays with mixed data types in their entries, while
 * DynamoDB does not. JSON also supports arrays of objects, which DynamoDB does not. Such items will raise {@link MappingException}
 * when saved to DynamoDB.
 *
 * This class does not implement all the DynamoDB methods-- additional methods taking a similar approach can be used
 * by using the DynamoDB low-level API and then using {@link #convert(Class, Map)} and {@link #convert(Object)} to
 * convert between the AttributeValue maps that the low-level API uses and the target class.
 *
 * In order to determine table name, hash key, and range key, the mapper traverses first the provided class, then its
 * superclasses, then its interfaces, looking for a class annotated with {@link DynamoDBTable}. That class must
 * have a method annotated with {@link DynamoDBHashKey} and, if the table has a range key, {@link DynamoDBRangeKey}.
 * In processing all three annotations, the mapper uses the annotations' values to determine the target table and
 * the object keys containing hash and range keys-- so the annotations must always be used with explicit `tableName`,
 * `attributeName`, and `attributeName` arguments respectively.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class JsonDynamoMapper {
    private final AmazonDynamoDB amazonDynamoDB;
    private final ObjectMapper objectMapper;

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB) {
        this(amazonDynamoDB, new ObjectMapper().registerModule(new Jdk8Module()));
    }

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB, ObjectMapper objectMapper) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.objectMapper = objectMapper;
    }

    public <T> PutItemResult save(T item) throws MappingException {
        JsonNode serialized = objectMapper.valueToTree(item);

        Class<?> clazz = item.getClass();

        Class<?> annotatedClazz = findAnnotatedClass(clazz, DynamoDBTable.class);
        if (annotatedClazz == null) {
            throw new MappingException("Could not find annotated interface for provided class " + clazz);
        } else {
            clazz = annotatedClazz;
        }

        String tableName = tableName(clazz);

        Method[] methods = clazz.getMethods();
        DynamoDBVersionAttribute versionAnnotation = null;
        for (Method method : methods) {
            if ((versionAnnotation = method.getAnnotation(DynamoDBVersionAttribute.class)) != null) {
                break;
            }
        }

        if (versionAnnotation != null) {
            return putItem(serialized, tableName, versionAnnotation.attributeName());
        }

        return putItem(serialized, tableName);
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey) throws MappingException {
        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(hashKeyAttribute(clazz), new AttributeValue().withS(hashKey)));

        if (item.getItem() == null) {
            return Optional.empty();
        }

        return Optional.of(convert(clazz, item.getItem()));
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey, String rangeKey) throws MappingException {
        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(
                        hashKeyAttribute(clazz), new AttributeValue().withS(hashKey),
                        rangeKeyAttribute(clazz), new AttributeValue().withS(rangeKey)));

        if (item.getItem() == null) {
            return Optional.empty();
        }

        return Optional.of(convert(clazz, item.getItem()));
    }

    public <T> ScanResultPage<T> scan(Class<T> clazz) throws MappingException {
        return scan(clazz, new DynamoDBScanExpression());
    }

    /**
     * Scan the table associated with specified class. All specifics of the scan should be specified in the provided
     * DynamoDBScanExpression
     * @return includes last evaluated key, for pagination
     * @throws MappingException On JSON errors or invalid class
     */
    public <T> ScanResultPage<T> scan(Class<T> clazz, @NotNull DynamoDBScanExpression scanExpression) throws MappingException {
        ScanResult scanResult = amazonDynamoDB.scan(scanRequestForScanExpression(scanExpression)
                .withTableName(tableName(clazz)));

        List<Map<String, AttributeValue>> items = scanResult.getItems();
        ImmutableList.Builder<T> objectListBuilder = new ImmutableList.Builder<>();
        for (Map<String, AttributeValue> item : items) {
            objectListBuilder.add(convert(clazz, item));
        }

        ScanResultPage<T> page = new ScanResultPage<>();
        page.setConsumedCapacity(scanResult.getConsumedCapacity());
        page.setCount(scanResult.getCount());
        page.setScannedCount(scanResult.getScannedCount());
        page.setLastEvaluatedKey(scanResult.getLastEvaluatedKey());
        page.setResults(objectListBuilder.build());
        return page;
    }

    /**
     * Basic method to convert a raw DynamoDB record (AttributeValue map) into an instance of a model class.
     * @param clazz Class to deserialize into
     * @param attributeValueMap Record contents from DynamoDB
     * @return  Instance of the T; not-null
     * @throws MappingException On JSON errors or illegal class
     */
    @NotNull
    public <T> T convert(Class<T> clazz, Map<String, AttributeValue> attributeValueMap) throws MappingException {
        ObjectNode converted = JsonNodeAttributeValueMapper.convert(attributeValueMap, objectMapper);
        try {
            return objectMapper.readValue(converted.traverse(), clazz);
        } catch (IOException e) {
            throw new MappingException("Exception deserializing", e);
        }
    }

    public <T> Map<String, AttributeValue> convert(T item) throws MappingException {
        JsonNode serialized = objectMapper.valueToTree(item);
        return JsonNodeAttributeValueMapper.convert(serialized);
    }

    @VisibleForTesting
    PutItemResult putItem(JsonNode jsonNode, String table) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);
        return amazonDynamoDB.putItem(new PutItemRequest().withTableName(table).withItem(attributeValueMap));
    }

    @VisibleForTesting
    PutItemResult putItem(JsonNode jsonNode, String table, @NotNull String versionField) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = JsonNodeAttributeValueMapper.convert(jsonNode);

        @Nullable AttributeValue currentVersion = attributeValueMap.get(versionField);

        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(table)
                .withExpressionAttributeNames(ImmutableMap.of("#v", versionField));

        if (currentVersion == null || currentVersion.getN() == null) {
            putItemRequest = putItemRequest.withConditionExpression("attribute_not_exists(#v)");
            ImmutableMap.Builder<String, AttributeValue> builder = new ImmutableMap.Builder<>();

            attributeValueMap.entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equals(versionField))
                    .forEach(e -> builder.put(e.getKey(), e.getValue()));

            builder.put(versionField, new AttributeValue().withN("1"));

            attributeValueMap = builder.build();
        } else {
            putItemRequest = putItemRequest.withConditionExpression("#v = :vf")
                    .withExpressionAttributeValues(ImmutableMap.of(":vf", new AttributeValue().withN(currentVersion.getN())));
            int v = Integer.parseInt(currentVersion.getN());
            currentVersion.setN(String.valueOf(v + 1));
        }

        return amazonDynamoDB.putItem(putItemRequest.withItem(attributeValueMap));
    }

    private <T> String tableName(Class<T> clazz) throws MappingException {
        @Nullable DynamoDBTable tableAnnotation = clazz.getAnnotation(DynamoDBTable.class);
        if (tableAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBTable.class);
        }

        return tableAnnotation.tableName();
    }

    private <T> String hashKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBHashKey hashKeyAnnotation = null;
        for (Method method : methods) {
            if ((hashKeyAnnotation = method.getAnnotation(DynamoDBHashKey.class)) != null) {
                break;
            }
        }

        if (hashKeyAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBHashKey.class);
        }

        return hashKeyAnnotation.attributeName();
    }

    private <T> String rangeKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBRangeKey rangeKeyAnnotation = null;
        for (Method method : methods) {
            if ((rangeKeyAnnotation = method.getAnnotation(DynamoDBRangeKey.class)) != null) {
                break;
            }
        }

        if (rangeKeyAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBRangeKey.class);
        }

        return rangeKeyAnnotation.attributeName();
    }

    private <T> Class<?> findAnnotatedClass(Class<T> clazz, Class<? extends Annotation> annotationClass) {
        if (clazz.getAnnotation(annotationClass) != null) {
            return clazz;
        }

        Class<?> superClazz = clazz.getSuperclass();
        if (superClazz != null) {
            Class<?> found = findAnnotatedClass(superClazz, annotationClass);
            if (found != null) {
                return found;
            }
        }

        for (Class<?> iface : clazz.getInterfaces()) {
            if (iface.getAnnotation(annotationClass) != null) {
                return iface;
            }
        }
        return null;
    }

    private static ScanRequest scanRequestForScanExpression(DynamoDBScanExpression scanExpression) {
        ScanRequest scanRequest = new ScanRequest();

        scanRequest.setIndexName(scanExpression.getIndexName());
        scanRequest.setScanFilter(scanExpression.getScanFilter());
        scanRequest.setLimit(scanExpression.getLimit());
        scanRequest.setExclusiveStartKey(scanExpression.getExclusiveStartKey());
        scanRequest.setTotalSegments(scanExpression.getTotalSegments());
        scanRequest.setSegment(scanExpression.getSegment());
        scanRequest.setConditionalOperator(scanExpression.getConditionalOperator());
        scanRequest.setFilterExpression(scanExpression.getFilterExpression());
        scanRequest.setExpressionAttributeNames(scanExpression
                .getExpressionAttributeNames());
        scanRequest.setExpressionAttributeValues(scanExpression
                .getExpressionAttributeValues());
        scanRequest.setSelect(scanExpression.getSelect());
        scanRequest.setProjectionExpression(scanExpression.getProjectionExpression());
        scanRequest.setReturnConsumedCapacity(scanExpression.getReturnConsumedCapacity());
        scanRequest.setConsistentRead(scanExpression.isConsistentRead());

        return scanRequest;
    }
}
