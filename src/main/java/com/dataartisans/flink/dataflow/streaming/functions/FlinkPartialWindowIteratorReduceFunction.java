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

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupCombineFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements VI, extracts the key and emits accumulated
 * values which have the intermediate format VA.
 */
public class FlinkPartialWindowIteratorReduceFunction<K, VI, VA> implements WindowMapFunction<KV<K, ? extends Iterable<VI>>, KV<K, VA>> {

	private final Combine.KeyedCombineFn<? super K, ? super VI, VA, ?> keyedCombineFn;

	public FlinkPartialWindowIteratorReduceFunction(Combine.KeyedCombineFn<? super K, ? super VI, VA, ?>
															keyedCombineFn) {
		this.keyedCombineFn = keyedCombineFn;
	}

	@Override
	public void mapWindow(Iterable<KV<K, ? extends Iterable<VI>>> elements, Collector<KV<K, VA>> out) throws Exception {
		final Iterator<? extends KV<K, ? extends Iterable<VI>>> elementsItarator = elements.iterator();
		// create accumulator using the first elements key
		KV<K, ? extends Iterable<VI>> first = elementsItarator.next();
		K key = first.getKey();
		Iterator<VI> valueIterator = first.getValue().iterator();
		VA accumulator = keyedCombineFn.createAccumulator(key);
		// manually add for the first element
		keyedCombineFn.addInput(key, accumulator, valueIterator.next());

		while(elementsItarator.hasNext()) {
			valueIterator = elementsItarator.next().getValue().iterator();
			while (valueIterator.hasNext()) {
				VI value = valueIterator.next();
				keyedCombineFn.addInput(key, accumulator, value);
			}
		}

		out.collect(KV.of(key, accumulator));
	}
}
