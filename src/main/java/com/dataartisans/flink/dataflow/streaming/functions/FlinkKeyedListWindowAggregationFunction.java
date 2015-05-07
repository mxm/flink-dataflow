/*
 * Copyright 2015 Data Artisans GmbH
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
package com.dataartisans.flink.dataflow.streaming.functions;

import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and collects
 * the values in a {@code List}.
 */
public class FlinkKeyedListWindowAggregationFunction<K,V> implements WindowMapFunction<KV<K, V>, KV<K, Iterable<V>>> {

	@Override
	public void mapWindow(Iterable<KV<K, V>> values, Collector<KV<K, Iterable<V>>> out) throws Exception {
		// This solution is suboptimal as it does a potentially avoidable pass over the window
		List<V> valueList = new ArrayList<>();
		for (KV<K, V> value : values){
			valueList.add(value.getValue());
		}
		out.collect(KV.of(values.iterator().next().getKey(), (Iterable<V>) valueList));
	}
}
