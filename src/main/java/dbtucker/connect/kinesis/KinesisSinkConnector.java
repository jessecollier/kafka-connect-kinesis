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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(KinesisSinkConnector.class);
  private KinesisSinkConnectorConfig config;

  @Override
  public void start(Map<String, String> map) {
    config = new KinesisSinkConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return KinesisSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.debug("Generating task configurations for {} tasks", maxTasks);
    return Collections.nCopies(maxTasks, config.originalsStrings());
  }

  @Override
  public void stop() {
    // No action necessary
  }

  @Override
  public ConfigDef config() {
    return KinesisSinkConnectorConfig.conf();
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

}
