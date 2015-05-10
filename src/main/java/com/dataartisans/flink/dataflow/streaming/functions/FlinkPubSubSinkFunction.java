/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.streaming.functions;

import com.dataartisans.flink.dataflow.util.PortableConfiguration;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Heavily motivated by https://cloud.google.com/pubsub/publisher,
// licensed Apache 2.0

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and merges the
 * accumulators resulting from the PartialReduce which produced the input VA.
 */
public class FlinkPubSubSinkFunction extends RichSinkFunction<String> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPubSubSinkFunction.class);

	private final String topic;
	private Pubsub pubsub;
	private PublishRequest publishRequest;
	private PubsubMessage pubsubMessage;

	public FlinkPubSubSinkFunction(String topic) {
		this.topic = topic;
	}

	public void open(Configuration parameters) throws Exception {
		pubsub = PortableConfiguration.createPubsubClient();
		pubsub.topics().create(new Topic().setName(topic)).execute();
		LOG.info("Created PubSub topic with name: {}", topic);

		publishRequest = new PublishRequest().setTopic(topic);
		pubsubMessage = new PubsubMessage();
	}

	@Override
	public void invoke(String message) throws Exception {
		pubsubMessage.encodeData(message.getBytes("UTF-8"));
		publishRequest.setMessage(pubsubMessage);

		pubsub.topics().publish(publishRequest).execute();
		LOG.debug("Sent message to PubSub: {}", message);
	}
}
