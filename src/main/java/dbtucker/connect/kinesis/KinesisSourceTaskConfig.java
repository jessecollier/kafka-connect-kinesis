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

import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KinesisSourceTaskConfig extends AbstractConfig {
    enum CfgKeys {
        ;
        static String REGION = "region";
        static String TOPIC_FORMAT = "topic.format";
        static String REC_PER_REQ = "records.per.request";
        static String SHARDS = "shards";
        static String STREAM = "stream";
        static String STREAM_ARN = "stream.arn";
    }

    private enum CfgTips {
        ;
        static final String REGION = "AWS region for the Kinesis source stream(s)." ;
        static final String TOPIC_FORMAT = "Format string for destination Kafka topic; use ``${stream}`` as placeholder for source stream name." ;
        static final String REC_PER_REQ = "max records per request" ;
        static final String SHARDS = "comma-separated list of fully qualified shards (format: <stream>/<shard>)" ;
        static final String STREAM = "unused" ;
        static final String STREAM_ARN = "unused" ;
    }

    private static final ConfigDef myConfigDef = new ConfigDef()
            .define(CfgKeys.REGION, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.LOW, CfgTips.REGION)
            .define(CfgKeys.TOPIC_FORMAT, ConfigDef.Type.STRING, "${stream}",
                    ConfigDef.Importance.HIGH, CfgTips.TOPIC_FORMAT)
            .define(CfgKeys.REC_PER_REQ, ConfigDef.Type.STRING, "${stream}",
                    ConfigDef.Importance.HIGH, CfgTips.REC_PER_REQ)
            .define(CfgKeys.SHARDS, ConfigDef.Type.STRING, "${stream}",
                    ConfigDef.Importance.HIGH, CfgTips.SHARDS)
            .define(CfgKeys.STREAM, ConfigDef.Type.STRING, "${stream}",
                    ConfigDef.Importance.HIGH, CfgTips.STREAM)
            .define(CfgKeys.STREAM_ARN, ConfigDef.Type.STRING, "${stream}",
                    ConfigDef.Importance.HIGH, CfgTips.STREAM_ARN);

    final List<String> shards;      // Syntax of each entry is <Stream>;<Shard> (so as to allow comma-separator)

    KinesisSourceTaskConfig(Map<String, String> props) {
        super(myConfigDef, (Map) props, true);
        shards = Arrays.stream(getString(CfgKeys.SHARDS).split(",")).filter(shardId -> !shardId.isEmpty()).collect(Collectors.toList());
    }

    public String getRegion() {
        return this.getString(CfgKeys.REGION);
    }

    public Regions getRegionId() {
        return Regions.fromName(this.getString(CfgKeys.REGION));
    }

    public String getTopicFormat(){
        return this.getString(CfgKeys.TOPIC_FORMAT);
    }

    public Integer getRecPerReq(){
        return (Integer.valueOf(getString(CfgKeys.REC_PER_REQ)));
    }

    public List<String> getShards() {
        return this.shards;
    }

}

