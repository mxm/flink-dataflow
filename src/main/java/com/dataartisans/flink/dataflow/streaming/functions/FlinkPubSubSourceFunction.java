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
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.Subscription;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


// Heavily motivated by https://cloud.google.com/pubsub/subscriber,
// licensed Apache 2.0

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and merges the
 * accumulators resulting from the PartialReduce which produced the input VA.
 */
public class FlinkPubSubSourceFunction implements SourceFunction<String> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPubSubSourceFunction.class);

	private final String topic;
	private volatile boolean isRunning = false;

	//TODO: figure out how to use this with parallelism
	public FlinkPubSubSourceFunction(String topic) {
		this.topic = topic;
	}

	@Override
	public void run(Collector<String> collector) throws Exception {
		isRunning = true;
		LOG.info("Flink PubSub SourceFunction started.");

		//Set up a PubSub connection
		Pubsub  pubsub = PortableConfiguration.createPubsubClient();
		Subscription subscription = new Subscription().setTopic(topic);
		Subscription newSubscribtion = pubsub.subscriptions().create(subscription).execute();
		LOG.info("Flink PubSub created new connection with name {}", newSubscribtion.getName());


		// Return immediately for smooth job cancellation
		PullRequest pullRequest = new PullRequest().setReturnImmediately(true);

		do {
			// Read data
			PullResponse pullResponse = pubsub.subscriptions().pull(pullRequest).execute();
			PubsubMessage message = pullResponse.getPubsubEvent().getMessage();
			if (message != null) {
				collector.collect(new String(message.decodeData(), "UTF-8"));
				LOG.debug("Received the following data: {}", new String(message.decodeData(), "UTF-8"));

				// Acknowledge received data
				// TODO: do async, batched ack for throughput
				List<String> ackList = new ArrayList<>();
				ackList.add(message.getMessageId());
				AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckId(ackList);
				pubsub.subscriptions().acknowledge(ackRequest);
			} else {
				LOG.debug("Received empty message.");
			}
		} while (isRunning);
	}

	@Override
	public void cancel() {
		isRunning = false;
		LOG.info("Flink PubSub SourceFunction cancelling.");
	}
}
