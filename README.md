# mapper
Simplified and simple usage of DynamoDB. Mapper assumes you've already defined reasonable marshaling of your class
to and from JSON, and simply persists objects to DynamoDB tables as records with structure that reflects the objects'
JSON representation.

This allows you to use libraries like [FreeBuilder](https://github.com/google/FreeBuilder) and [AutoValue](https://github.com/google/auto/tree/master/value) to get clean Java classes with immutable instances
and minimal boilerplate, while still maintaining the simple DynamoDB persistence afforded by the [DynamoDB object mapper](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.html).

<a href="https://travis-ci.org/scopely/mapper"><img src="https://travis-ci.org/scopely/mapper.svg" /></a>
[ ![Download](https://api.bintray.com/packages/scopely-oss/scopely-maven/mapper/images/download.svg) ](https://bintray.com/scopely-oss/scopely-maven/mapper/_latestVersion)

This library is hosted on Bintray, to use it in your build, add the Maven repository to your repositories list:

```groovy
repositories {
    maven {
        url  "http://dl.bintray.com/scopely-oss/scopely-maven"
    }
}
```


## Examples

Mapper supports the version attribute for [optimistic locking](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html:

```java
@FreeBuilder
@JsonDeserialize(builder = SimpleFreeBuiltVersioned.Builder.class)
@DynamoDBTable(tableName = "simple_free_built_versioned")
interface SimpleFreeBuiltVersioned {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();
    String getStringValue();
    @DynamoDBVersionAttribute(attributeName = "version")
    Optional<Integer> getVersion();

    class Builder extends SimpleFreeBuiltVersioned_Builder {}
}
```

Note that the example above also uses `Optional<T>` -- the optional is omitted from the JSON by Jackson, so it is safely
round-tripped to DynamoDB.

Mapper also supports specifying a range key:

```java
@FreeBuilder
@JsonDeserialize(builder = HashAndRange.Builder.class)
@DynamoDBTable(tableName = "hash_and_range")
interface HashAndRange {
    @DynamoDBHashKey(attributeName = "hashKey")
    String getHashKey();
    @DynamoDBRangeKey(attributeName = "rangeKey")
    String getRangeKey();

    class Builder extends HashAndRange_Builder {}
}
```

And in most cases vanilla Java objects will continue to work with Mapper, just like they do with the [SDK's built-in
`DynamoDBMapper`](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.html).

The API of mapper is simply the class `JsonDynamoMapper`:

```java
public <T> PutItemResult save(T item) throws MappingException {}
public <T> Optional<T> load(Class<T> clazz, String hashKey) throws MappingException {}
public <T> Optional<T> load(Class<T> clazz, String hashKey, String rangeKey) throws MappingException {}
public <T> ScanResultPage<T> scan(Class<T> clazz) throws MappingException {}
public <T> ScanResultPage<T> scan(Class<T> clazz, @NotNull DynamoDBScanExpression scanExpression) throws MappingException {}
public <T> T convert(Class<T> clazz, Map<String, AttributeValue> attributeValueMap) throws MappingException {}
public <T> Map<String, AttributeValue> convert(T item) throws MappingException {}
```

The basics of `save`, `load` and `scan` are effectively the same as the built-in object mapper. More DynamoDB operations
can be implemented by using the simple `convert` methods in conjunction the the [DynamoDB SDK's low-level API](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBClient.html).