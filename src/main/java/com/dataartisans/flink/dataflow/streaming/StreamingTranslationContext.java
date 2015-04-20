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
package com.dataartisans.flink.dataflow.streaming;

import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.types.KvCoderTypeInformation;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class StreamingTranslationContext {

	private final Map<PValue, DataStream<?>> dataStreams;
//	private final Map<PCollectionView<?>, DataSet<?>> broadcastDataSets;

	private final StreamExecutionEnvironment env;
	private final PipelineOptions options;

	// ------------------------------------------------------------------------

	public StreamingTranslationContext(StreamExecutionEnvironment env, PipelineOptions options) {
		this.env = env;
		this.options = options;
		this.dataStreams = new HashMap<>();
//		this.broadcastDataSets = new HashMap<>();
	}
	
	// ------------------------------------------------------------------------
	
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return env;
	}

	public PipelineOptions getPipelineOptions() {
		return options;
	}
	
	@SuppressWarnings("unchecked")
	public <T> DataStream<T> getInputDataStream(PValue value) {
		return (DataStream<T>) dataStreams.get(value);
	}
	
	public void setOutputDataStream(PValue value, DataStream<?> stream) {
		if (!dataStreams.containsKey(value)) {
			dataStreams.put(value, stream);
		}
	}

//	@SuppressWarnings("unchecked")
//	public <T> DataSet<T> getSideInputDataSet(PCollectionView<?> value) {
//		return (DataSet<T>) broadcastDataSets.get(value);
//	}
//
//	public void setSideInputDataSet(PCollectionView<?> value, DataSet<?> set) {
//		if (!broadcastDataSets.containsKey(value)) {
//			broadcastDataSets.put(value, set);
//		}
//	}
	
	@SuppressWarnings("unchecked")
	public <T> TypeInformation<T> getTypeInfo(PValue output) {
		if (output instanceof TypedPValue) {
			Coder<?> outputCoder = ((TypedPValue) output).getCoder();
			if (outputCoder instanceof KvCoder) {
				return new KvCoderTypeInformation((KvCoder) outputCoder);
			} else {
				return new CoderTypeInformation(outputCoder);
			}
		}
		return new GenericTypeInfo<T>((Class<T>)Object.class);
	}
}
