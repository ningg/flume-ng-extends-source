/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ningg.flume.source;

/**
 * Refer to {@link https://github.com/apache/flume/tree/trunk/flume-ng-sources/flume-kafka-source}
 * @author Ning Guo
 * @time 2015-03-09
 */

public class KafkaSourceConstants {
  public static final String TOPIC = "topic";
  public static final String KEY = "key";
  public static final String TIMESTAMP = "timestamp";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION_MS = "batchDurationMillis";
  public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";
  public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_FLUME = "zookeeperConnect";
  public static final String GROUP_ID = "group.id";
  public static final String GROUP_ID_FLUME = "groupId";
  public static final String PROPERTY_PREFIX = "kafka.";


  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int DEFAULT_BATCH_DURATION = 1000;
  public static final String DEFAULT_CONSUMER_TIMEOUT = "10";
  public static final String DEFAULT_AUTO_COMMIT =  "false";
  public static final String DEFAULT_GROUP_ID = "flume";

}