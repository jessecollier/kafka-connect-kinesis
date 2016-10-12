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


public class KinesisSinkConnectorConfig extends AbstractConfig {

  private enum CfgKeys {
  	;
    static final String REGION = "region" ;
    static final String STREAM_FORMAT = "stream.format" ;
    static final String MAX_RETRIES = "max.retries";
    static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  }

  private enum CfgTips {
  	;
    static final String REGION = "AWS region for the Kinesis source stream(s)." ;
    static final String STREAM_FORMAT = "Format string for destination Kinesis stream; use ``${topic}`` as placeholder for source topic name." ;
    static final String MAX_RETRIES = "The maximum number of times to retry on errors writing to Kinesis before failing the task.";
    static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  }

  private static final ConfigDef myConfigDef = new ConfigDef()
      .define(CfgKeys.REGION, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (key, regionName) -> {
         if (!Arrays.stream(Regions.values()).anyMatch(x -> x.getName().equals(regionName))) {
            throw new ConfigException("Invalid AWS region: " + regionName);
         }
      }, ConfigDef.Importance.HIGH, CfgTips.REGION)
      .define(CfgKeys.STREAM_FORMAT, ConfigDef.Type.STRING, "${topic}",
         ConfigDef.Importance.HIGH, CfgTips.STREAM_FORMAT)
      .define(CfgKeys.MAX_RETRIES, ConfigDef.Type.INT, 10,
         ConfigDef.Importance.MEDIUM, CfgTips.MAX_RETRIES)
      .define(CfgKeys.RETRY_BACKOFF_MS, ConfigDef.Type.INT, 3000,
         ConfigDef.Importance.MEDIUM, CfgTips.RETRY_BACKOFF_MS) ;

  public KinesisSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public KinesisSinkConnectorConfig(Map<String, String> parsedConfig) {
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

  public String getStreamFormat() {
    return this.getString(CfgKeys.STREAM_FORMAT);
  }

  public Integer getMaxRetries() {
    return this.getInt(CfgKeys.MAX_RETRIES);
  }

  public Integer getRetryBackoffMs() { 
    return this.getInt(CfgKeys.RETRY_BACKOFF_MS);
  }
}
