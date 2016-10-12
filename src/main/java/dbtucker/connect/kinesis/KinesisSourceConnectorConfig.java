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
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class KinesisSourceConnectorConfig extends AbstractConfig {

  private enum CfgKeys {
  	;
    static final String REGION = "region" ;
    static final String STREAMS_PREFIX = "streams.prefix" ;
    static final String STREAMS_WHITELIST = "streams.whitelist" ;
    static final String STREAMS_BLACKLIST = "streams.blacklist" ;
    static final String TOPIC_FORMAT = "topic.format" ;
  }

  private enum CfgTips {
  	;
    static final String REGION = "AWS region for the Kinesis source stream(s)." ;
    static final String STREAMS_PREFIX = "Prefix for Kinesis streams from which records will be retrieved." ;
    static final String STREAMS_WHITELIST = "Whitelist for Kinesis streams from which records will be sourced." ;
    static final String STREAMS_BLACKLIST = "Blacklist for Kinesis streams from which records will be sourced." ;
    static final String TOPIC_FORMAT = "Format string for destination Kafka topic; use ``${stream}`` as placeholder for source stream name." ;
  }

  private static final ConfigDef myConfigDef = new ConfigDef()
      .define(CfgKeys.REGION, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (key, regionName) -> {
         if (!Arrays.stream(Regions.values()).anyMatch(x -> x.getName().equals(regionName))) {
            throw new ConfigException("Invalid AWS region: " + regionName);
         }
      }, ConfigDef.Importance.HIGH, CfgTips.REGION)
      .define(CfgKeys.STREAMS_PREFIX, ConfigDef.Type.STRING, null,
         ConfigDef.Importance.MEDIUM, CfgTips.STREAMS_PREFIX)
      .define(CfgKeys.STREAMS_WHITELIST, ConfigDef.Type.LIST, null,
         ConfigDef.Importance.MEDIUM, CfgTips.STREAMS_WHITELIST)
      .define(CfgKeys.STREAMS_BLACKLIST, ConfigDef.Type.LIST, null,
         ConfigDef.Importance.MEDIUM, CfgTips.STREAMS_BLACKLIST)
      .define(CfgKeys.TOPIC_FORMAT, ConfigDef.Type.STRING, "${stream}",
         ConfigDef.Importance.HIGH, CfgTips.TOPIC_FORMAT);

        // We'll keep local copies of these since the values are used
        // in a loop elsewhere, and it makes no sense to rebuild
        // a List for multiple loop iterations.
  final List<String> streamsWhitelist;
  final List<String> streamsBlacklist;


  public KinesisSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);

    streamsWhitelist = this.getList(CfgKeys.STREAMS_WHITELIST);
    streamsBlacklist = this.getList(CfgKeys.STREAMS_BLACKLIST);
  }

  public KinesisSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return myConfigDef;
  }

  public String getRegion() {
    return this.getString(CfgKeys.REGION);
  }

  public Regions getRegionId() {
    return Regions.fromName(this.getString(CfgKeys.REGION));
  }

  public String getStreamsPrefix() {
    return this.getString(CfgKeys.STREAMS_PREFIX);
  }

  public List<String> getStreamsWhitelist() {
    return this.streamsWhitelist;
  }

  public List<String> getStreamsBlacklist() {
    return this.streamsBlacklist;
  }

  public String getTopicFormat(){
    return this.getString(CfgKeys.TOPIC_FORMAT);
  }
}
