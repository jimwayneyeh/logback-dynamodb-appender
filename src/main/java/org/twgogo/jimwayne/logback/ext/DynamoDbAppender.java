package org.twgogo.jimwayne.logback.ext;

import static java.lang.String.format;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.eluder.logback.ext.aws.core.AbstractAwsEncodingStringAppender;
import org.eluder.logback.ext.aws.core.AwsSupport;
import org.eluder.logback.ext.aws.core.LoggingEventHandler;
import org.eluder.logback.ext.core.AppenderExecutors;
import org.eluder.logback.ext.core.StringPayloadConverter;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;

public class DynamoDbAppender extends AbstractAwsEncodingStringAppender<ILoggingEvent, String> {
  private static final int DEFAULT_MAX_PAYLOAD_SIZE = 384;
  private static final JsonParser jsonParser = new JsonParser();

  private String region;
  private String table;
  private List<String> partitionKeys = new LinkedList<String>();
  private String rangeKey;

  private AmazonDynamoDBAsyncClient dynamoDb;

  public DynamoDbAppender() {
    super();
    System.out.println("Initializing DynamoDbAppender");
    setMaxPayloadSize(DEFAULT_MAX_PAYLOAD_SIZE);
  }

  protected DynamoDbAppender(AwsSupport awsSupport, Filter<ILoggingEvent> sdkLoggingFilter) {
    super(awsSupport, sdkLoggingFilter);
    System.out.println("Initializing DynamoDbAppender with parameters");
    setMaxPayloadSize(DEFAULT_MAX_PAYLOAD_SIZE);
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setPartitionKey(String partitionKey) {
    System.out.println("Add partition key: " + partitionKey);
    this.partitionKeys.add(partitionKey);
  }

  public void setRangeKey(String rangeKey) {
    this.rangeKey = rangeKey;
  }

  @Override
  public void start() {
    if (getEncoder() == null) {
      GsonEncoder encoder = new GsonEncoder();
      setEncoder(encoder);
    }
    setConverter(new StringPayloadConverter(getCharset(), isBinary()));
    super.start();
  }

  @Override
  protected void doStart() {
    dynamoDb = new AmazonDynamoDBAsyncClient(
        getCredentials(), 
        getClientConfiguration(),
        AppenderExecutors.newExecutor(this, getThreadPoolSize()));
    dynamoDb.setRegion(RegionUtils.getRegion(region));
  }

  @Override
  protected void doStop() {
    if (dynamoDb != null) {
      AppenderExecutors.shutdown(this, dynamoDb.getExecutorService(), getMaxFlushTime());
      dynamoDb.shutdown();
      dynamoDb = null;
    }
  }

  @Override
  protected void handle (final ILoggingEvent event, final String encoded) throws Exception {
    Item item = Item.fromJSON(encoded).withPrimaryKey(this.createEventId(encoded));
    System.out.println("Log: " + item);
    Map<String, AttributeValue> attributes = InternalUtils.toAttributeValues(item);
    PutItemRequest request = new PutItemRequest(table, attributes);
    String errorMessage =
        format("Appender '%s' failed to send logging event '%s' to DynamoDB table '%s'", getName(),
            event, table);
    CountDownLatch latch = new CountDownLatch(isAsyncParent() ? 0 : 1);
    dynamoDb.putItemAsync(request,
        new LoggingEventHandler<PutItemRequest, PutItemResult>(this, latch, errorMessage));
    AppenderExecutors.awaitLatch(this, latch, getMaxFlushTime());
  }

  protected PrimaryKey createEventId (String encoded) {
    JsonObject json = jsonParser.parse(encoded).getAsJsonObject();
    
    String keyName = StringUtils.EMPTY;
    String valueName = StringUtils.EMPTY;
    
    Iterator<String> pkIter = this.partitionKeys.iterator();
    while (pkIter.hasNext()) {
      String currentKey = pkIter.next();
      
      // Check if the requested field for partition key is exist.
      if (!json.has(currentKey)) {
        throw new IllegalArgumentException(
            String.format("No '%s' is found in the encoded logs.",
                  currentKey));
      }
      
      if (StringUtils.isEmpty(keyName)) {
        keyName = currentKey;
        valueName = json.get(currentKey).getAsString();
      } else {
        keyName += "_" + currentKey;
        valueName += "_" + json.get(currentKey).getAsString();
      }
    }
    
    // No range key is needed.
    if (StringUtils.isEmpty(this.rangeKey)) {
      return new PrimaryKey(keyName, valueName);
    }
    
    // Check if the requested field for partition key is exist.
    if (!json.has(this.rangeKey)) {
      throw new IllegalArgumentException(
          String.format("No '%s' is found in the encoded logs.",
              this.rangeKey));
    }
    
    return new PrimaryKey(
        keyName, valueName, 
        this.rangeKey, json.get(this.rangeKey).getAsString());
  }

}
