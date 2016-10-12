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
import com.amazonaws.services.kinesis.model.Record;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KinesisSourceTask extends SourceTask {
  private enum Keys {
    ;
    static final String SHARD = "shard";
    static final String SEQNUM = "seqNum";
  }

  private final Logger log = LoggerFactory.getLogger(getClass());

  private KinesisSourceTaskConfig config;
  private AmazonKinesisClient client;;

  private List<String> assignedShards;
  private Map<String, String> shardIterators;
  private int currentShardIdx;

  @Override
  public void start(Map<String, String> props) {
    config = new KinesisSourceTaskConfig(props);

    client = new AmazonKinesisClient();
    client.configureRegion(config.getRegionId());

    assignedShards = new ArrayList<>(config.getShards());
    shardIterators = new HashMap<>(assignedShards.size());
    currentShardIdx = 0;

    log.info("start: KinesisSourceTaskConfiguration values: {}", config.originalsStrings());
    log.info("start: {} shards assigned to task {}", assignedShards.size(), this.toString());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (assignedShards.isEmpty()) {
        throw new ConnectException("No source shards remaining for this task");
    }

      // Kinesis best practice is to sleep 1 second between calls to getRecords
      // We'll do that only after we've cycled through all the shards we're polling
      if (currentShardIdx == 0) {
          try {
              Thread.sleep(1000);
          } catch (InterruptedException exception) {
              throw exception;
          }
      }

    final String shardUid = assignedShards.get(currentShardIdx);

    final GetRecordsRequest req = new GetRecordsRequest();
    req.setShardIterator(toShardIterator(shardUid));
    req.setLimit(config.getRecPerReq()); 

    final GetRecordsResult rsp = client.getRecords(req);
    log.info("client.getRecords retrieve {} records", rsp.getRecords().size());
    log.debug("client.getRecords returns {}", rsp.toString());
    if (rsp.getNextShardIterator() == null) {
        log.info("Shard ID `{}` for stream `{}` has been closed, it will no longer be polled",
                shardUid.split("/")[1], shardUid.split("/")[0]);
        shardIterators.remove(shardUid);
        assignedShards.remove(shardUid);
    } else {
        shardIterators.put(shardUid, rsp.getNextShardIterator());
    }

    currentShardIdx = (currentShardIdx + 1) % assignedShards.size();

    final String streamName = shardUid.split("/")[0];
    final String topic = config.getTopicFormat().replace("${stream}", streamName);
    final Map<String, String> sourcePartition = toSourcePartition(shardUid);

    return rsp.getRecords().stream()
            .map(kinesisRecord -> toSourceRecord(sourcePartition, topic, kinesisRecord))
            .collect(Collectors.toList());
  }

  private SourceRecord toSourceRecord(Map<String, String> sourcePartition, String topic, Record kinesisRecord) {
        // TODO propagate timestamp via 
        // `dynamoRecord.getApproximateCreationDateTime.getTime` 
        // when that's exposed by Connect
    byte xferData[] = kinesisRecord.getData().array();

    log.debug("Raw kinesis record key {}", kinesisRecord.getPartitionKey().toString());
    log.debug("Raw kinesis data {}", kinesisRecord.getData().toString());
    log.debug("Extracted kinesis data {}", xferData.toString());

    return new SourceRecord(
        sourcePartition,
        Collections.singletonMap(Keys.SEQNUM, kinesisRecord.getSequenceNumber()),
        topic,
        RecordMapper.keySchema(), 
        RecordMapper.packKey(kinesisRecord.getPartitionKey()),
        RecordMapper.dataSchema(), 
        RecordMapper.packData(kinesisRecord.getData())
    );
  }

  private String toShardIterator(String shardUid) {
    String iterator = shardIterators.get(shardUid);
    if (iterator == null) {
        String thisStream = shardUid.split("/")[0];
        String thisShard = shardUid.split("/")[1];
        log.debug("Initializing iterator for shardId {} of stream {} ", thisShard, thisStream);
        final GetShardIteratorRequest req =
          getShardIteratorRequest ( thisShard, thisStream,
            storedSequenceNumber(toSourcePartition(shardUid)) );

    iterator = client.getShardIterator(req).getShardIterator();
    shardIterators.put(shardUid, iterator);
    }
    return iterator;
  }

  private GetShardIteratorRequest getShardIteratorRequest(
                String shardId,
                String streamName,
                String seqNum
    ) {
        final GetShardIteratorRequest req = new GetShardIteratorRequest();
        req.setShardId(shardId);
        req.setStreamName(streamName);
        if (seqNum == null) {
            req.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            req.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            req.setStartingSequenceNumber(seqNum);
        }
        return req;
  }

  private Map<String, String> toSourcePartition(String shardUid) {
    return Collections.singletonMap(Keys.SHARD, shardUid);
  }

  private String storedSequenceNumber(Map<String, String> partition) {
    final Map<String, Object> offsetMap = 
        context.offsetStorageReader().offset(partition);

    return offsetMap != null ? (String) offsetMap.get(Keys.SEQNUM) : null;
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
