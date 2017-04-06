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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  @Override
  public void start(Map<String, String> map) {
    config = new KinesisSinkConnectorConfig(map);
    client = new AmazonKinesisClient();
    client.configureRegion(config.getRegionId());
    remainingRetries = config.getMaxRetries();
    log.debug("Task launched with client {}", client.toString());
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
        final String defaultPartitionKey = record.topic() + "_" + record.kafkaPartition().toString();
        List<PutRecordsRequestEntry> writes = writesByStream.get(streamName);
        if (writes == null) {
            writes = new ArrayList<>();
            writesByStream.put(streamName, writes);
        }

        final PutRecordsRequestEntry put = new PutRecordsRequestEntry();
        put.setData(ByteBuffer.wrap(record.value().toString().getBytes()));
        if (record.key() != null) {
            if (record.keySchema() != null) {
                // TODO: correctly parse schema'ed key
                put.setPartitionKey(record.key().toString());   // assume toString handles real Strings correctly
            } else {
                put.setPartitionKey(record.key().toString());   // assume toString handles real Strings correctly
            }
        } else {

            put.setPartitionKey(defaultPartitionKey);
        }

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
          log.debug("putRecords returns {}", putRecordsResult.toString());

          entries = entries.subList(maxRecordsPerRequest,entries.size()-1);
          numEntries = entries.size();
      }
      if (numEntries > 0) {
          putRecordsRequest.setRecords(entries);
          putRecordsResult = client.putRecords(putRecordsRequest);
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

}
