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

import com.dataartisans.flink.dataflow.translation.TranslationContext;
import com.dataartisans.flink.dataflow.translation.functions.FlinkFlatMapDoFnFunction;
import com.dataartisans.flink.dataflow.translation.functions.FlinkKeyedListWindowAggregationFunction;
import com.dataartisans.flink.dataflow.translation.functions.FlinkPartialWindowReduceFunction;
import com.dataartisans.flink.dataflow.translation.functions.FlinkWindowReduceFunction;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FileSourceFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Translators for transforming
 * Dataflow {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s to
 * Flink {@link org.apache.flink.api.java.DataSet}s
 */
public class FlinkStreamingTransformTranslators {

	private static boolean hasUnAppliedWindow = false;
	private static WindowFn<?, ?> windowFn;

	// --------------------------------------------------------------------------------------------
	//  Transform Translator Registry
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends PTransform>, FlinkStreamingPipelineTranslator.TransformTranslator> TRANSLATORS = new HashMap<>();

	// register the known translators
	static {
		// we don't need this because we translate the Combine.PerKey directly
		// TRANSLATORS.put(Combine.GroupedValues.class, new CombineGroupedValuesTranslator());

		TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundTranslator());

		TRANSLATORS.put(Window.Bound.class, new WindowBoundTranslator());

		TRANSLATORS.put(GroupByKey.GroupByKeyOnly.class, new GroupByKeyOnlyTranslator());
		TRANSLATORS.put(GroupByKey.ReifyTimestampsAndWindows.class, new ReifyTimestampAndWindowsTranslator());
		TRANSLATORS.put(GroupByKey.GroupAlsoByWindow.class, new GroupAlsoByWindowsTranslator());
		TRANSLATORS.put(GroupByKey.SortValuesByTimestamp.class, new SortValuesByTimestampTranslator());

		//TRANSLATORS.put(BigQueryIO.Read.Bound.class, null);
		//TRANSLATORS.put(BigQueryIO.Write.Bound.class, null);

		//TRANSLATORS.put(DatastoreIO.Sink.class, null);

		//TRANSLATORS.put(PubsubIO.Read.Bound.class, null);
		//TRANSLATORS.put(PubsubIO.Write.Bound.class, null);

//		TRANSLATORS.put(ReadSource.Bound.class, new ReadSourceTranslator());

		TRANSLATORS.put(TextIO.Read.Bound.class, new TextIOReadTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteTranslator());
	}


	public static FlinkStreamingPipelineTranslator.TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}

//	private static class ReadSourceTranslator<T> implements FlinkStreamingPipelineTranslator.TransformTranslator<ReadSource.Bound<T>> {
//
//		@Override
//		public void translateNode(ReadSource.Bound<T> transform, StreamingTranslationContext context) {
//			String name = transform.getName();
//			Source<T> source = transform.getSource();
//			Coder<T> coder = transform.getOutput().getCoder();
//
//			TypeInformation<T> typeInformation = context.getTypeInfo(transform.getOutput());
//
//			// TODO: Add DataStreamSource accordingly
////			DataStreamSource<T> dataSource = new DataStreamSource<>(context.getExecutionEnvironment(), new SourceInputFormat<>(source, context.getPipelineOptions(), coder), typeInformation, name);
////
////			context.setOutput(transform.getOutput(), dataSource);
//		}
//	}


	private static class TextIOReadTranslator implements FlinkStreamingPipelineTranslator.TransformTranslator<TextIO.Read.Bound<String>> {
		private static final Logger LOG = LoggerFactory.getLogger(TextIOReadTranslator.class);

		@Override
		public void translateNode(TextIO.Read.Bound<String> transform, StreamingTranslationContext context) {
			String path = transform.getFilepattern();
			String name = transform.getName();

			TextIO.CompressionType compressionType = transform.getCompressionType();
			boolean needsValidation = transform.needsValidation();

			// TODO: Implement these. We need Flink support for this.
			LOG.warn("Translation of TextIO.CompressionType not yet supported. Is: {}.", compressionType);
			LOG.warn("Translation of TextIO.Read.needsValidation not yet supported. Is: {}.", needsValidation);

			InputFormat<String, ?> inputFormat = new TextInputFormat(new Path(path));
			TypeInformation<String> typeInformation = context.getTypeInfo(transform.getOutput());

			// TODO: Add DataStreamSource accordingly
//			DataSource<String> source = new DataSource<>(context.getExecutionEnvironment(), new TextInputFormat(new Path(path)), typeInformation, name);

			DataStreamSource<String> source = new DataStreamSource<>(context.getExecutionEnvironment(), "source",
					typeInformation, new StreamSource<>(new FileSourceFunction(inputFormat, typeInformation)), true, name);
			context.getExecutionEnvironment().getStreamGraph().setInputFormat(source.getId(), inputFormat);

			context.setOutputDataStream(transform.getOutput(), source);
		}
	}

	private static class TextIOWriteTranslator<T> implements FlinkStreamingPipelineTranslator.TransformTranslator<TextIO.Write.Bound<T>> {
		private static final Logger LOG = LoggerFactory.getLogger(TextIOWriteTranslator.class);

		@Override
		public void translateNode(TextIO.Write.Bound<T> transform, StreamingTranslationContext context) {
			DataStream<T> inputDataStream = context.getInputDataStream(transform.getInput());
			String filenamePrefix = transform.getFilenamePrefix();
			String filenameSuffix = transform.getFilenameSuffix();
			boolean needsValidation = transform.needsValidation();
			int numShards = transform.getNumShards();
			String shardNameTemplate = transform.getShardNameTemplate();

			// TODO: Implement these. We need Flink support for this.
			LOG.warn("Translation of TextIO.Write.needsValidation not yet supported. Is: {}.", needsValidation);
			LOG.warn("Translation of TextIO.Write.filenameSuffix not yet supported. Is: {}.", filenameSuffix);
			LOG.warn("Translation of TextIO.Write.shardNameTemplate not yet supported. Is: {}.", shardNameTemplate);

			inputDataStream.print();
//			DataSink<T> dataSink = inputDataStream.writeAsText(filenamePrefix);
//
//			if (numShards > 0) {
//				dataSink.setParallelism(numShards);
//			}
		}
	}

	private static class ParDoBoundTranslator<IN, OUT> implements FlinkStreamingPipelineTranslator.TransformTranslator<ParDo.Bound<IN, OUT>> {
		private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslator.class);

		@Override
		public void translateNode(ParDo.Bound<IN, OUT> transform, StreamingTranslationContext context) {
			DataStream<IN> inputDataStream = context.getInputDataStream(transform.getInput());

			final DoFn<IN, OUT> doFn = transform.getFn();

//			if (doFn instanceof DoFn.RequiresKeyedState) {
//				LOG.error("Flink Batch Execution does not support Keyed State.");
//			}

			TypeInformation<OUT> typeInformation = context.getTypeInfo(transform.getOutput());

			FlinkFlatMapDoFnFunction<IN, OUT> doFnWrapper = new FlinkFlatMapDoFnFunction<>(doFn, context.getPipelineOptions());
//			MapPartitionOperator<IN, OUT> outputDataSet = new MapPartitionOperator<>(inputDataStream, typeInformation, doFnWrapper, transform.getName());

			DataStream<OUT> outputDataStream = inputDataStream.transform(transform.getName(), typeInformation, new StreamFlatMap<>(doFnWrapper));

//			transformSideInputs(transform.getSideInputs(), outputDataSet, context);

			context.setOutputDataStream(transform.getOutput(), outputDataStream);
		}
	}

	private static class WindowBoundTranslator<T> implements FlinkStreamingPipelineTranslator.TransformTranslator<Window.Bound<T>>{

		@Override
		public void translateNode(Window.Bound<T> transform, StreamingTranslationContext context) {
			hasUnAppliedWindow = true;
			windowFn = transform.getWindowFn();

			// TODO: Add windowing at the beginning of group by key and combine by key
//			DataStream<T> inputDataStream = context.getInputDataStream(transform.getInput());
//			DataStream<T> outputDataStream = inputDataStream.window(Time.of(1, TimeUnit.SECONDS)).flatten();

			context.setOutputDataStream(transform.getOutput(), context.getInputDataStream(transform.getInput()));
		}
	}

	private static class GroupByKeyOnlyTranslator<K, V> implements FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.GroupByKeyOnly<K, V>>, Serializable {

		@Override
		public void translateNode(GroupByKey.GroupByKeyOnly<K, V> transform, StreamingTranslationContext context) {
			DataStream<KV<K,V>> inputDataStream = context.getInputDataStream(transform.getInput());

			if (!hasUnAppliedWindow){
				throw new UnsupportedOperationException("Cannot group unbound data flows.");
			} else {
				// TODO: properly get window function
				DataStream outputDataStream = inputDataStream.window(Time.of(1, TimeUnit.SECONDS)).groupBy(new KeySelector<KV<K,V>, K>() {
					@Override
					public K getKey(KV<K, V> kv) throws Exception {
						return kv.getKey();
					}
				}).mapWindow(new FlinkKeyedListWindowAggregationFunction<K, V>()).flatten();

				context.setOutputDataStream(transform.getOutput(), outputDataStream);
			}

		}
	}

	private static class CombinePerKeyTranslator<K, VI, VA, VO> implements FlinkStreamingPipelineTranslator.TransformTranslator<Combine.PerKey<K, VI, VO>> {

		@Override
		public void translateNode(Combine.PerKey<K, VI, VO> transform, StreamingTranslationContext context) {
			DataStream<KV<K,VI>> inputDataStream = context.getInputDataStream(transform.getInput());

			@SuppressWarnings("unchecked")
			Combine.KeyedCombineFn<K, VI, VA, VO> keyedCombineFn = (Combine.KeyedCombineFn<K, VI, VA, VO>) transform.getFn();

			if (!hasUnAppliedWindow){
				throw new UnsupportedOperationException("Cannot group unbound data flows.");
			} else {
				FlinkPartialWindowReduceFunction<K, VI, VA> partialReduceFunction = new FlinkPartialWindowReduceFunction<>(keyedCombineFn);
				FlinkWindowReduceFunction<K, VA, VO> reduceFunction = new FlinkWindowReduceFunction<>(keyedCombineFn);

				// TODO: properly get window function
				DataStream outputDataStream = inputDataStream.window(Time.of(1, TimeUnit.SECONDS))
						// Partially reduce to the intermediate format VA
						.mapWindow(partialReduceFunction)
						// Fully reduce the values and create output format VO
						.mapWindow(reduceFunction)
						.flatten();
				context.setOutputDataStream(transform.getOutput(), outputDataStream);
			}
		}
	}

	private static class GroupAlsoByWindowsTranslator<K, V> implements
			FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.GroupAlsoByWindow<K, V>> {

		// not Flink's way, this would do the grouping by window, maybe we should apply the windowing here
		@Override
		public void translateNode(GroupByKey.GroupAlsoByWindow<K, V> transform, StreamingTranslationContext context) {
			DataStream<KV<K, Iterable<WindowedValue<V>>>> inputDataStream = context.getInputDataStream(transform.getInput());

			DataStream outputDataStream = inputDataStream.map(new MapFunction<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>>() {
				List<V> nonWindowedValues = new ArrayList<>();
				@Override
				public KV<K, Iterable<V>> map(KV<K, Iterable<WindowedValue<V>>> kv) throws Exception {
					nonWindowedValues.clear();
					for (WindowedValue<V> windowedValue : kv.getValue()){
						nonWindowedValues.add(windowedValue.getValue());
					}
					return KV.of(kv.getKey(), (Iterable<V>) nonWindowedValues);
				}
			});

			context.setOutputDataStream(transform.getOutput(), outputDataStream);
		}
	}

	private static class ReifyTimestampAndWindowsTranslator<K, V> implements
			FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.ReifyTimestampsAndWindows<K, V>>{

		@Override
		public void translateNode(GroupByKey.ReifyTimestampsAndWindows<K, V> transform, final StreamingTranslationContext context) {
			DataStream<KV<K,V>> inputDataStream = context.getInputDataStream(transform.getInput());

			DataStream outputDataStream = inputDataStream.map(new MapFunction<KV<K,V>, KV<K, WindowedValue<V>>>() {
				@Override
				public KV<K, WindowedValue<V>> map(KV<K, V> kv) throws Exception {
					// Windowed value is not Flink's way
					// TODO: use more dummy time
					return KV.of(kv.getKey(), WindowedValue.of(kv.getValue(), Instant.now(), new ArrayList<BoundedWindow>()));
				}
			});

			context.setOutputDataStream(transform.getOutput(), outputDataStream);
		}
	}

	private static class SortValuesByTimestampTranslator<K, V> implements
			FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.SortValuesByTimestamp<K, V>>{

		//This is a no-op in Flink
		@Override
		public void translateNode(GroupByKey.SortValuesByTimestamp<K, V> transform, StreamingTranslationContext context) {
			context.setOutputDataStream(transform.getOutput(), context.getInputDataStream(transform.getInput()));
		}
	}

	private static void transformSideInputs(List<PCollectionView<?>> sideInputs,
											MapPartitionOperator<?, ?> outputDataSet,
											TranslationContext context) {
		// get corresponding Flink broadcast DataSets
		for(PCollectionView<?> input : sideInputs) {
			DataSet<?> broadcastSet = context.getSideInputDataSet(input);
			outputDataSet.withBroadcastSet(broadcastSet, input.getTagInternal().getId());
		}
	}


	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	private FlinkStreamingTransformTranslators() {}
}
