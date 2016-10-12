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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(KinesisSourceConnector.class);
  private KinesisSourceConnectorConfig config;
  private Map<Shard, DescribeStreamResult> streamShards;

  @Override
  public void start(Map<String, String> map) {
    config = new KinesisSourceConnectorConfig(map);
    streamShards = new HashMap<>() ;

    List<String> streamNames;
    final Set<String> ignoredStreams = new HashSet<>();
    final Set<String> consumedStreams = new HashSet<>();

    final AmazonKinesisClient client = new AmazonKinesisClient();
    client.configureRegion(config.getRegionId());

    ListStreamsResult listResult;
    ListStreamsRequest lsr = new ListStreamsRequest();
    lsr.setLimit(32);

    String lastEvaluatedStreamName = null;
    do {
        lsr.setExclusiveStartStreamName(lastEvaluatedStreamName);
        listResult = client.listStreams(lsr);

        streamNames = listResult.getStreamNames() ;
        for (String streamName : streamNames) {
            if (config.getStreamsPrefix() == null) {
                if ((config.getStreamsBlacklist() == null || config.getStreamsBlacklist().contains(streamName)) &&
                    (config.getStreamsWhitelist() == null || !config.getStreamsWhitelist().contains(streamName))) {
                    ignoredStreams.add(streamName);
                    continue;
                }
            } else {
                if (streamName.startsWith(config.getStreamsPrefix())) {
                    if (config.getStreamsBlacklist() != null && config.getStreamsBlacklist().contains(streamName)) {
                        ignoredStreams.add(streamName);
                        continue;
                    }
                } else {
                    ignoredStreams.add(streamName);
                    continue;
                }
            }

            final DescribeStreamResult streamDesc = client.describeStream(streamName);

            if (streamDesc.getStreamDescription().getStreamStatus().equals(StreamStatus.DELETING.toString())) {
                log.warn("Stream '{}' is being deleted and cannot be consumed", streamName);
                ignoredStreams.add(streamName);
                continue;
            }

            for (Shard shard: streamDesc.getStreamDescription().getShards()) {
                streamShards.put(shard, streamDesc);
            }

            consumedStreams.add(streamName);
        }

        if (streamNames.size() > 0) {
            lastEvaluatedStreamName = streamNames.get(streamNames.size() - 1);
        }

    } while (listResult.getHasMoreStreams());

    log.info("Streams to ingest: {}", consumedStreams);
    log.info("Streams to ignore: {}", ignoredStreams);

    client.shutdown();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return KinesisSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return ConnectorUtils.groupPartitions(
      new ArrayList<>(streamShards.keySet()), maxTasks).stream().map(taskShards -> {
        final Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put(KinesisSourceTaskConfig.CfgKeys.REGION, config.getRegion());
        taskConfig.put(KinesisSourceTaskConfig.CfgKeys.REC_PER_REQ, "10");
        taskConfig.put(KinesisSourceTaskConfig.CfgKeys.TOPIC_FORMAT, config.getTopicFormat());
        taskConfig.put(KinesisSourceTaskConfig.CfgKeys.SHARDS, taskShards.stream().map(Shard::getShardId).collect(Collectors.joining(",")));
        taskShards.forEach(shard -> {
                final StreamDescription streamDesc = streamShards.get(shard).getStreamDescription();
                taskConfig.put(shard.getShardId() + "." + KinesisSourceTaskConfig.CfgKeys.STREAM, streamDesc.getStreamName());
                taskConfig.put(shard.getShardId() + "." + KinesisSourceTaskConfig.CfgKeys.STREAM_ARN, streamDesc.getStreamARN());
        });
        return taskConfig;
    }).collect(Collectors.toList());
  }

  @Override
  public void stop() {
    //No action needed; closing connection will be fine
  }

  @Override
  public ConfigDef config() {
    return KinesisSourceConnectorConfig.conf();
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

}
