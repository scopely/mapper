package com.scopely.mapper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
    private final int BATCH_SAVE_UNPROCESSED_MAX_TRIES = 20;

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;
    private final ObjectMapper objectMapper;

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB, DynamoDB dynamoDb) {
        this(amazonDynamoDB, dynamoDb, new ObjectMapper().registerModule(new Jdk8Module()));
    }

    public JsonDynamoMapper(AmazonDynamoDB amazonDynamoDB, DynamoDB dynamoDb, ObjectMapper objectMapper) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = dynamoDb;
        this.objectMapper = objectMapper;
    }

    public <T> T saveAndGet(T item) throws MappingException {
        PutItemResult putItemResult = save(item);
        return convert((Class<T>) item.getClass(), putItemResult.getAttributes());
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

        List<String> autoGeneratedAttributes = autoGeneratedAttributes(clazz);

        Method[] methods = clazz.getMethods();
        DynamoDBVersionAttribute versionAnnotation;
        for (Method method : methods) {
            if ((versionAnnotation = method.getAnnotation(DynamoDBVersionAttribute.class)) != null) {
                if (versionAnnotation.attributeName().trim().isEmpty()) {
                    throw new MappingException("Class " + clazz + " missing attributeName for annotation " + DynamoDBVersionAttribute.class);
                }
                return putItem(serialized, tableName, autoGeneratedAttributes, versionAnnotation.attributeName());
            }
        }

        return putItem(serialized, tableName, autoGeneratedAttributes);
    }

    public <T> void saveAll(Class<T> clazz, List<T> items) throws MappingException {
        Class<?> annotatedClazz = findAnnotatedClass(clazz, DynamoDBTable.class);
        if (annotatedClazz == null) {
            throw new MappingException("Could not find annotated interface for provided class " + clazz);
        }

        String tableName = tableName(clazz);
        List<String> autoGeneratedAttributes = autoGeneratedAttributes(clazz);

        TableWriteItems tableWriteItems = new TableWriteItems(tableName);
        for (T item : items) {
            JsonNode json = objectMapper.valueToTree(item);
            Map<String, AttributeValue> attributeValueMap = generateKeys(JsonNodeAttributeValueMapper.convert(json),
                    autoGeneratedAttributes);

            tableWriteItems.addItemToPut(Item.fromMap(InternalUtils.toSimpleMapValue(attributeValueMap)));
        }

        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
        int tries = 0;
        do {
            if (tries++ > BATCH_SAVE_UNPROCESSED_MAX_TRIES) {
                throw new MappingException("Reached max number of tries to execute batch save for unprocessed items");
            }

            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            if (outcome.getUnprocessedItems().size() > 0) {
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }

        } while (outcome.getUnprocessedItems().size() > 0);
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey) throws MappingException {
        return load(clazz, hashKey, true);
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey, boolean consistentRead) throws MappingException {
        if (hashKey == null || hashKey.trim().isEmpty()) {
            throw new IllegalArgumentException("HashKey can't be null or empty");
        }

        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(hashKeyAttribute(clazz), new AttributeValue().withS(hashKey)),
                consistentRead);

        if (item.getItem() == null) {
            return Optional.empty();
        }

        return Optional.of(convert(clazz, item.getItem()));
    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey, String rangeKey) throws MappingException {
        return load(clazz, hashKey, rangeKey, true);

    }

    public <T> Optional<T> load(Class<T> clazz, String hashKey, String rangeKey, boolean consistentRead) throws MappingException {
        if (hashKey == null || hashKey.trim().isEmpty()) {
            throw new IllegalArgumentException("HashKey can't be null or empty");
        }

        if (rangeKey == null || rangeKey.trim().isEmpty()) {
            throw new IllegalArgumentException("RangeKey can't be null or empty");
        }

        GetItemResult item = amazonDynamoDB.getItem(tableName(clazz),
                ImmutableMap.of(
                        hashKeyAttribute(clazz), new AttributeValue().withS(hashKey),
                        rangeKeyAttribute(clazz), new AttributeValue().withS(rangeKey)),
                consistentRead);

        if (item.getItem() == null) {
            return Optional.empty();
        }

        return Optional.of(convert(clazz, item.getItem()));
    }

    public <T> void delete(Class<T> clazz, String hashKey) throws MappingException {
        if (hashKey == null || hashKey.trim().isEmpty()) {
            throw new IllegalArgumentException("HashKey can't be null or empty");
        }

        amazonDynamoDB.deleteItem(
                tableName(clazz),
                ImmutableMap.of(hashKeyAttribute(clazz), new AttributeValue().withS(hashKey)));
    }

    public <T> void delete(Class<T> clazz, String hashKey, String rangeKey) throws MappingException {
        if (rangeKey == null || rangeKey.trim().isEmpty()) {
            throw new IllegalArgumentException("RangeKey can't be null or empty");
        }

        amazonDynamoDB.deleteItem(tableName(clazz),
                ImmutableMap.of(
                        hashKeyAttribute(clazz), new AttributeValue().withS(hashKey),
                        rangeKeyAttribute(clazz), new AttributeValue().withS(rangeKey)));
    }

    public <T> ScanResultPage<T> scan(Class<T> clazz) throws MappingException {
        return scan(clazz, new DynamoDBScanExpression());
    }

    public <T> List<T> scanAll(Class<T> clazz) throws MappingException {
        List<T> results = new ArrayList<>();

        ScanResultPage<T> scan = scan(clazz, new DynamoDBScanExpression());
        results.addAll(scan.getResults());

        while (scan.getLastEvaluatedKey() != null) {
            scan = scan(clazz,
                        new DynamoDBScanExpression()
                                .withExclusiveStartKey(scan.getLastEvaluatedKey()));
            results.addAll(scan.getResults());
        }

        return results;
    }

    /**
     * Scan the table associated with specified class. All specifics of the scan should be specified in the provided
     * DynamoDBScanExpression
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
     * Queries the table associated with specified class. All specifics of the scan should be specified in the provided
     * DynamoDBQueryExpression
     * @throws MappingException On JSON errors or invalid class
     */
    public <T> List<T> queryAll(Class<T> clazz, @NotNull DynamoDBQueryExpression queryExpr) throws MappingException {
        List<T> results = new ArrayList<>();

        QueryResultPage<T> scan = query(clazz, queryExpr);
        results.addAll(scan.getResults());

        while (scan.getLastEvaluatedKey() != null) {
            scan = query(clazz,
                         queryExpr.withExclusiveStartKey(scan.getLastEvaluatedKey()));
            results.addAll(scan.getResults());
        }

        return results;
    }


    /**
     * Queries the table associated with specified class. All specifics of the scan should be specified in the provided
     * DynamoDBQueryExpression
     * @return includes last evaluated key, for pagination
     * @throws MappingException On JSON errors or invalid class
     */
    public <T> QueryResultPage<T> query(Class<T> clazz, @NotNull DynamoDBQueryExpression queryExpr) throws MappingException {
        QueryResult queryResult = amazonDynamoDB.query(queryRequestForScanExpression(queryExpr)
                                                            .withTableName(tableName(clazz)));

        List<Map<String, AttributeValue>> items = queryResult.getItems();
        ImmutableList.Builder<T> objectListBuilder = new ImmutableList.Builder<>();
        for (Map<String, AttributeValue> item : items) {
            objectListBuilder.add(convert(clazz, item));
        }

        QueryResultPage<T> page = new QueryResultPage<>();
        page.setConsumedCapacity(queryResult.getConsumedCapacity());
        page.setCount(queryResult.getCount());
        page.setScannedCount(queryResult.getScannedCount());
        page.setLastEvaluatedKey(queryResult.getLastEvaluatedKey());
        page.setResults(objectListBuilder.build());
        return page;
    }


    /**
     * Basic method to convert a raw DynamoDB record (AttributeValue map) into an instance of a model class.
     *
     * @param clazz             Class to deserialize into
     * @param attributeValueMap Record contents from DynamoDB
     *
     * @return Instance of the T; not-null
     * @throws MappingException On JSON errors or illegal class
     */
    @NotNull
    public <T> T convert(Class<T> clazz, Map<String, AttributeValue> attributeValueMap) throws MappingException {
        ObjectNode converted = JsonNodeAttributeValueMapper.convert(attributeValueMap, objectMapper);
        try {
            return objectMapper.readValue(converted.traverse(), clazz);
        } catch (IOException e) {
            throw new MappingException("Exception deserializing: " + converted, e);
        }
    }

    public <T> Map<String, AttributeValue> convert(T item) throws MappingException {
        JsonNode serialized = objectMapper.valueToTree(item);
        return JsonNodeAttributeValueMapper.convert(serialized);
    }

    @VisibleForTesting
    PutItemResult putItem(JsonNode jsonNode, String table, List<String> autoGeneratedKeys) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = generateKeys(JsonNodeAttributeValueMapper.convert(jsonNode), autoGeneratedKeys);
        return amazonDynamoDB.putItem(new PutItemRequest().withTableName(table).withItem(attributeValueMap))
                             .withAttributes(attributeValueMap);
    }

    @VisibleForTesting
    PutItemResult putItem(JsonNode jsonNode, String table, List<String> autoGeneratedKeys, @NotNull String versionField) throws MappingException {
        Map<String, AttributeValue> attributeValueMap = generateKeys(JsonNodeAttributeValueMapper.convert(jsonNode), autoGeneratedKeys);

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

        return amazonDynamoDB.putItem(putItemRequest.withItem(attributeValueMap))
                             .withAttributes(attributeValueMap);
    }

    /**
     * Generate keys for null attributes
     */
    private static Map<String, AttributeValue> generateKeys(Map<String, AttributeValue> attrMap, List<String> autoGeneratedKeys) {
        if (autoGeneratedKeys.isEmpty()) {
            return attrMap;
        }

        Map<String, AttributeValue> newAttrBuilder = new HashMap<>();
        newAttrBuilder.putAll(attrMap);

        for (String autoGeneratedKey : autoGeneratedKeys) {
            AttributeValue attributeValue = attrMap.get(autoGeneratedKey);
            if (attributeValue == null || attributeValue.getS() == null) {
                newAttrBuilder.put(autoGeneratedKey, new AttributeValue(UUID.randomUUID().toString()));
            }
        }

        return Collections.unmodifiableMap(newAttrBuilder);
    }

    private static <T> String tableName(Class<T> clazz) throws MappingException {
        @Nullable DynamoDBTable tableAnnotation = clazz.getAnnotation(DynamoDBTable.class);
        if (tableAnnotation == null) {
            throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBTable.class);
        }

        return tableAnnotation.tableName();
    }

    private static <T> String hashKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBHashKey hashKeyAnnotation;
        for (Method method : methods) {
            if ((hashKeyAnnotation = method.getAnnotation(DynamoDBHashKey.class)) != null) {
                String attributeName = hashKeyAnnotation.attributeName();

                if (attributeName.trim().isEmpty()) {
                    throw new MappingException("Class " + clazz + " missing attributeName for annotation " + DynamoDBHashKey.class);
                }

                return attributeName;
            }
        }

        throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBHashKey.class);
    }

    private static <T> String rangeKeyAttribute(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();
        DynamoDBRangeKey rangeKeyAnnotation;
        for (Method method : methods) {
            if ((rangeKeyAnnotation = method.getAnnotation(DynamoDBRangeKey.class)) != null) {
                String attributeName = rangeKeyAnnotation.attributeName();

                if (attributeName.trim().isEmpty()) {
                    throw new MappingException("Class " + clazz + " missing attributeName for annotation " + DynamoDBRangeKey.class);
                }

                return attributeName;
            }
        }

        throw new MappingException("Class " + clazz + " missing required annotation " + DynamoDBRangeKey.class);
    }

    private static <T> List<String> autoGeneratedAttributes(Class<T> clazz) throws MappingException {
        Method[] methods = clazz.getMethods();

        ImmutableList.Builder<String> autoGeneratedKeys = ImmutableList.builder();
        for (Method method : methods) {
            DynamoDBAutoGeneratedKey autoGeneratedKey = method.getAnnotation(DynamoDBAutoGeneratedKey.class);
            if (autoGeneratedKey != null) {
                if (method.getReturnType() != String.class && !isValidOptionalType(method)) {
                    throw new MappingException("DynamoDBAutoGeneratedKey is only supported for the String and java.util.Optional<String> types: " + method);
                }

                DynamoDBHashKey hashKeyAnnotation = method.getAnnotation(DynamoDBHashKey.class);
                if (hashKeyAnnotation != null) {
                    autoGeneratedKeys.add(hashKeyAnnotation.attributeName());
                    continue;
                }

                DynamoDBRangeKey rangeKeyAnnotation = method.getAnnotation(DynamoDBRangeKey.class);
                if (rangeKeyAnnotation != null) {
                    autoGeneratedKeys.add(rangeKeyAnnotation.attributeName());
                } else {
                    throw new MappingException("DynamoDBAutoGeneratedKey used in " + method + " but it wasn't a hash or range key");
                }
            }
        }

        return autoGeneratedKeys.build();
    }

    private static boolean isValidOptionalType(Method method) {
        if (method.getReturnType() != Optional.class) {
            return false;
        }

        AnnotatedType annotatedReturnType = method.getAnnotatedReturnType();
        Type type = annotatedReturnType.getType();

        try {
            Class.forName("sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl");
        } catch (ClassNotFoundException e) {
            // if this class does not exists, we can't check for the actual parameters
            return false;
        }

        if (type instanceof ParameterizedTypeImpl) {
            Type parameterType = ((ParameterizedTypeImpl) type).getActualTypeArguments()[0];
            String typeName = parameterType.getTypeName();
            if (String.class.getName().equals(typeName)) {
                return true;
            }
        }

        return false;
    }

    @Nullable
    private static <T> Class<?> findAnnotatedClass(Class<T> clazz, Class<? extends Annotation> annotationClass) {
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

    private static QueryRequest queryRequestForScanExpression(DynamoDBQueryExpression queryExpr) {
        QueryRequest queryRequest = new QueryRequest();

        queryRequest.setIndexName(queryExpr.getIndexName());
        queryRequest.setQueryFilter(queryExpr.getQueryFilter());
        queryRequest.setKeyConditionExpression(queryExpr.getKeyConditionExpression());
        queryRequest.setKeyConditions(queryExpr.getRangeKeyConditions());
        queryRequest.setScanIndexForward(queryExpr.isScanIndexForward());
        queryRequest.setExpressionAttributeValues(queryExpr.getExpressionAttributeValues());
        queryRequest.setLimit(queryExpr.getLimit());
        queryRequest.setExclusiveStartKey(queryExpr.getExclusiveStartKey());
        queryRequest.setConditionalOperator(queryExpr.getConditionalOperator());
        queryRequest.setFilterExpression(queryExpr.getFilterExpression());
        queryRequest.setExpressionAttributeNames(queryExpr
                                                        .getExpressionAttributeNames());
        queryRequest.setExpressionAttributeValues(queryExpr
                                                         .getExpressionAttributeValues());
        queryRequest.setSelect(queryExpr.getSelect());
        queryRequest.setProjectionExpression(queryExpr.getProjectionExpression());
        queryRequest.setReturnConsumedCapacity(queryExpr.getReturnConsumedCapacity());
        queryRequest.setConsistentRead(queryExpr.isConsistentRead());

        return queryRequest;
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
