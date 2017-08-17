/*
 * Copyright 2016 David Tucker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbtucker.connect.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KinesisSinkTask extends SinkTask {
  private final Logger log = LoggerFactory.getLogger(KinesisSinkTask.class);

  private KinesisSinkConnectorConfig config;
  private AmazonKinesisClient client;
  private int remainingRetries;
  private int partitionKeysCount = 1_000_000;
  private Random random = new Random();

  @Override
  public void start(Map<String, String> map) {
    config = new KinesisSinkConnectorConfig(map);
    client = new AmazonKinesisClient();
    client.configureRegion(config.getRegionId());
    remainingRetries = config.getMaxRetries();
    log.debug("Task launched with client {}", client.toString());
  }

    // Use the key provided in the kafka message. Does not support avro schemas
    // Returns random key as default
  public void getPartitionKey(SinkRecord record) {
    final String record_key = record.key() == null ? "" : record.key().toString();
    if (!record_key.isEmpty()) {
      return record_key;
    } else {
      return Integer.toString(random.nextInt(partitionKeysCount));
    }
  }

    // Since Kinesis only supports ByteBuffers for the data sent down, we should invest a bit of
    // time in what to pass (and then how to parse the return).
    //
    //  For a JSON-serialized message (published without Schema enabled),
    //    SinkRecord.toString returns bytes that eventually decode to:
    //      SinkRecord{kafkaOffset=8} ConnectRecord{topic='vtest1', kafkaPartition=0, key=null, value={foo=baz, my=world}}
    //    SinkRecord.value().toString returns bytes that eventually decode to:
    //      {foo=baz, my=world}
    //
  @Override
  public void put(Collection<SinkRecord> collection) {
    if (collection.isEmpty()) return;
    final Map<String, List<PutRecordsRequestEntry>> writesByStream = new HashMap<>();
    for (SinkRecord record : collection) {
        
        final String streamName = config.getStreamFormat().replace("${topic}", record.topic());

        List<PutRecordsRequestEntry> writes = writesByStream.get(streamName);
        if (writes == null) {
            writes = new ArrayList<>();
            writesByStream.put(streamName, writes);
        }

        final PutRecordsRequestEntry put = new PutRecordsRequestEntry();
        put.setData(ByteBuffer.wrap(record.value().toString().getBytes()));

        final String partition_key = getPartitionKey(record);
        log.debug("Setting partition key: " + key);
        put.setPartitionKey(partition_key);
        writes.add(put.clone());
    }

    writesByStream.forEach((stream, reqEntries) -> {
        try {
            putRecords(stream, reqEntries);
        }
        catch (Exception e){
            log.warn("Write to stream {} failed; remaining retries={}", stream, remainingRetries);
            if (remainingRetries == 0) {
                throw new ConnectException(e);
            } else {
                remainingRetries-- ;
                context.timeout(config.getRetryBackoffMs());
                throw new RetriableException(e);
            }
        }
    });

    remainingRetries = config.getMaxRetries();
  }

  private void putRecords (String streamName, List<PutRecordsRequestEntry> entries) {
        // TODO  Get smarter about cleaning up memory from PutRecordRequestEntries
      log.debug("Sending {} records to stream {}", entries.size(), streamName);
      final Integer maxRecordsPerRequest = 100;  // AWS limit is 500, for now
      Integer numEntries;
      PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
      PutRecordsResult putRecordsResult;
      List<PutRecordsRequestEntry> entriesSubset;

      putRecordsRequest.setStreamName(streamName);
      numEntries = entries.size();
      while (numEntries > maxRecordsPerRequest) {
          entriesSubset = entries.subList(0, maxRecordsPerRequest-1);
          putRecordsRequest.setRecords(entriesSubset);
          putRecordsResult = client.putRecords(putRecordsRequest);
          try {
              retryPutRecords(streamName, entriesSubset, putRecordsResult);
          } catch (InterruptedException e) {
              log.error("InterruptedException caught on retry send", e);
          }
          log.debug("putRecords returns {}", putRecordsResult.toString());

          entries = entries.subList(maxRecordsPerRequest,entries.size()-1);
          numEntries = entries.size();
      }
      if (numEntries > 0) {
          putRecordsRequest.setRecords(entries);
          putRecordsResult = client.putRecords(putRecordsRequest);
          try {
              retryPutRecords(streamName, entries, putRecordsResult);
          } catch (InterruptedException e) {
              log.error("InterruptedException caught on retry send", e);
          }
          log.debug("putRecords returns {}", putRecordsResult.toString());
      }
  }


  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
  }

  @Override
  public void stop() {
    if (client != null) {
        client.shutdown();
        client = null;
    }
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  // http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html
  private void retryPutRecords(String streamName, List<PutRecordsRequestEntry> entries, PutRecordsResult putRecordsResult) throws InterruptedException {
      
      int retryCount = 0;
      int maxSleepValue = 6;
      while (putRecordsResult.getFailedRecordCount() > 0) {
          
          final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
          final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
          for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
              final PutRecordsRequestEntry putRecordRequestEntry = entries.get(i);
              final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
              if (putRecordsResultEntry.getErrorCode() != null) {
                  log.error("putRecords returned error: (" + putRecordsResultEntry.getErrorCode() + ") " + putRecordsResultEntry.getErrorMessage());
                  failedRecordsList.add(putRecordRequestEntry);
              }
          }

          entries = failedRecordsList; // reduce entries to new list of failed records
          PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
          putRecordsRequest.setStreamName(streamName);
          putRecordsRequest.setRecords(entries);

          // exponential sleep backoff
          int sleepvalue = retryCount > maxSleepValue ? (int) Math.pow(2, maxSleepValue) : (int)  Math.pow(2, retryCount);
          log.info("retryPutRecords: Sleeping for " + sleepvalue);
          Thread.sleep(sleepvalue);
          
          putRecordsResult = client.putRecords(putRecordsRequest);
          log.debug("retryPutRecords: Full result: " + putRecordsResult.toString());
          log.error("retryPutRecords: Retried " + failedRecordsList.size() + " records.");
          retryCount++;
      }

      log.info("retryPutRecords: Successfully resent all messages with errors.");
  }
}
