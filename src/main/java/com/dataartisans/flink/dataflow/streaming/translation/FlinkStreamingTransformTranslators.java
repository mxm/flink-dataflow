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
package com.dataartisans.flink.dataflow.streaming.translation;

import com.dataartisans.flink.dataflow.io.ConsoleIO;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkPubSubSinkFunction;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkPubSubSourceFunction;
import com.dataartisans.flink.dataflow.translation.TranslationContext;
import com.dataartisans.flink.dataflow.translation.functions.FlinkFlatMapDoFnFunction;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkKeyedListWindowAggregationFunction;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkPartialWindowIteratorReduceFunction;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkPartialWindowReduceFunction;
import com.dataartisans.flink.dataflow.streaming.functions.FlinkWindowReduceFunction;
import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.types.KvCoderTypeInformation;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DiscretizedStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileSourceFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
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
	private static boolean hasUnClosedWindow = false;
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

		TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslator());
		TRANSLATORS.put(Combine.GroupedValues.class, new CombineGroupedValuesTranslator());

		//TRANSLATORS.put(BigQueryIO.Read.Bound.class, null);
		//TRANSLATORS.put(BigQueryIO.Write.Bound.class, null);

		//TRANSLATORS.put(DatastoreIO.Sink.class, null);

		TRANSLATORS.put(PubsubIO.Read.Bound.class, null);
		//TRANSLATORS.put(PubsubIO.Write.Bound.class, null);

//		TRANSLATORS.put(ReadSource.Bound.class, new ReadSourceTranslator());

		TRANSLATORS.put(TextIO.Read.Bound.class, new TextIOReadTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteTranslator());

		// Flink-specific
		TRANSLATORS.put(ConsoleIO.Write.Bound.class, new ConsoleIOWriteTranslator());
	}


	public static FlinkStreamingPipelineTranslator.TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}

	private static WindowedDataStream applyWindow(DataStream inputDataStream){
		WindowedDataStream outputDataStream;

		if (windowFn instanceof FixedWindows) {
//			reflection troubles for final value
//			Duration size;
//			try {
//			Field field = windowFn.getClass().getDeclaredField("size");
//			field.setAccessible(true);
//			Class<?> targetType = field.getType();
//			Object objectValue = targetType.getConstructor(long.class)
//					.newInstance(0L);
//			size = (Duration) field.get(objectValue);
//			} catch (Exception e) {
//				throw  new RuntimeException(e);
//			}
//			outputDataStream = inputDataStream.window(Time.of(size.getMillis(), TimeUnit.MILLISECONDS));
			outputDataStream = inputDataStream.window(Time.of(1, TimeUnit.SECONDS));
		} else if (windowFn instanceof SlidingWindows) {
//			Duration size;
//			Duration period;
//			try {
//				Field field = windowFn.getClass().getDeclaredField("size");
//				field.setAccessible(true);
//				Class<?> targetType = field.getType();
//				Object objectValue = targetType.getConstructor(Long.class).newInstance(0L);
//				size = (Duration) field.get(objectValue);
//
//				field = windowFn.getClass().getDeclaredField("period");
//				field.setAccessible(true);
//				period = (Duration) field.get(objectValue);
//
//			} catch (Exception e) {
//				throw  new RuntimeException(e);
//			}
//			outputDataStream = inputDataStream.window(Time.of(size.getMillis(), TimeUnit.MILLISECONDS)).every(Time.of(period.getMillis(), TimeUnit.MILLISECONDS));
			outputDataStream = inputDataStream.window(Time.of(10, TimeUnit.SECONDS));
		} else {
			throw new UnsupportedOperationException("Currently only Fixed and Sliding windows are supported.");
		}
		return outputDataStream;
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

	// TODO: try out
	private static class PubSubIOReadTranslator implements FlinkStreamingPipelineTranslator.TransformTranslator<PubsubIO.Read.Bound>{

		@Override
		public void translateNode(PubsubIO.Read.Bound transform, StreamingTranslationContext context) {
			String topic = transform.getTopic();
			String name = transform.getName();

			TypeInformation<String> typeInformation = context.getTypeInfo(transform.getOutput());

			DataStreamSource<String> source = new DataStreamSource<>(context.getExecutionEnvironment(), "source",
					typeInformation, new StreamSource<>(new FlinkPubSubSourceFunction(topic)), true, name);

			context.setOutputDataStream(transform.getOutput(), source);
		}
	}

	// TODO: try out, consider generics
	private static class PubSubIOWriteTranslator implements FlinkStreamingPipelineTranslator.TransformTranslator<PubsubIO.Write.Bound>{

		@Override
		public void translateNode(PubsubIO.Write.Bound transform, StreamingTranslationContext context) {
			String topic = transform.getTopic();
			String name = transform.getName();

			DataStream<String> inputDataStream = context.getInputDataStream(transform.getInput());

			DataStream<String> dataSink = inputDataStream.addSink(new FlinkPubSubSinkFunction(topic)).name(name);
		}
	}

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
			String name = transform.getName();
			String filenamePrefix = transform.getFilenamePrefix();
			String filenameSuffix = transform.getFilenameSuffix();
			boolean needsValidation = transform.needsValidation();
			int numShards = transform.getNumShards();
			String shardNameTemplate = transform.getShardNameTemplate();

			// TODO: Implement these. We need Flink support for this.
			LOG.warn("Translation of TextIO.Write.needsValidation not yet supported. Is: {}.", needsValidation);
			LOG.warn("Translation of TextIO.Write.filenameSuffix not yet supported. Is: {}.", filenameSuffix);
			LOG.warn("Translation of TextIO.Write.shardNameTemplate not yet supported. Is: {}.", shardNameTemplate);

//			inputDataStream.print();
			DataStreamSink<T> dataSink = inputDataStream.writeAsText(filenamePrefix);
			dataSink.name(name);

			if (numShards > 0) {
				dataSink.setParallelism(numShards);
			}
		}
	}

	private static class ConsoleIOWriteTranslator implements FlinkStreamingPipelineTranslator.TransformTranslator<ConsoleIO.Write.Bound> {
		@Override
		public void translateNode(ConsoleIO.Write.Bound transform, StreamingTranslationContext context) {
			DataStream<?> inputDataStream = context.getInputDataStream(transform.getInput());
			inputDataStream.print();
		}
	}

	private static class ParDoBoundTranslator<IN, OUT> implements FlinkStreamingPipelineTranslator.TransformTranslator<ParDo.Bound<IN, OUT>> {
		private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslator.class);

		@Override
		public void translateNode(ParDo.Bound<IN, OUT> transform, StreamingTranslationContext context) {
			DataStream<IN> inputDataStream = context.getInputDataStream(transform.getInput());

			final DoFn<IN, OUT> doFn = transform.getFn();

			// TODO: handle keyed state
//			if (doFn instanceof DoFn.RequiresKeyedState) {
//				LOG.error("Flink Batch Execution does not support Keyed State.");
//			}

			TypeInformation<OUT> typeInformation = context.getTypeInfo(transform.getOutput());

			FlinkFlatMapDoFnFunction<IN, OUT> doFnWrapper = new FlinkFlatMapDoFnFunction<>(doFn, context.getPipelineOptions());

			DataStream<OUT> outputDataStream = inputDataStream.transform(transform.getName(), typeInformation, new StreamFlatMap<>(doFnWrapper));

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
			DataStream<KV<K, V>> inputDataStream = context.getInputDataStream(transform.getInput());

			// TODO: consider grouping unbound
			if (!hasUnAppliedWindow) {
				throw new UnsupportedOperationException("Cannot group unbound data flows.");
			} else {
				KvCoder<K, V> inputCoder = (KvCoder<K, V>) transform.getInput().getCoder();
				TypeInformation<KV<K,V>> inputType = new KvCoderTypeInformation<>(inputCoder);
				TypeInformation<KV<K, Iterable<V>>> outputType = context.getTypeInfo(transform.getOutput());
				ExecutionConfig config = inputDataStream.getExecutionEnvironment().getConfig();

//				GroupedDataStream<KV<K, V>> groupedStream = inputDataStream.groupBy(new KVKeySelector<K, V>());
				GroupedDataStream<KV<K, V>> groupedStream = new GroupedDataStream<>(inputDataStream,
						KeySelectorUtil.getSelectorForKeys(new Keys.ExpressionKeys<>(new String[]{"key"}, inputType), inputType, config));
				WindowedDataStream<KV<K, V>> windowedStream = applyWindow(groupedStream);
				DiscretizedStream<KV<K, Iterable<V>>> discretizedStream = windowedStream
						.mapWindow(new FlinkKeyedListWindowAggregationFunction<K, V>(), outputType)
						.name(transform.getName());

				// TODO: Support for passing windowed datastreams
				context.setOutputDataStream(transform.getOutput(), discretizedStream.flatten());
			}
		}
	}


	private static class CombinePerKeyTranslator<K, VI, VA, VO> implements FlinkStreamingPipelineTranslator.TransformTranslator<Combine.PerKey<K, VI, VO>> {

		@Override
		public void translateNode(Combine.PerKey<K, VI, VO> transform, StreamingTranslationContext context) {
			DataStream<KV<K,VI>> inputDataStream = context.getInputDataStream(transform.getInput());
			String partialOperatorName = transform.getName() + "-partial";
			String totalOperatorName = transform.getName() + "-total";

			@SuppressWarnings("unchecked")
			Combine.KeyedCombineFn<K, VI, VA, VO> keyedCombineFn = (Combine.KeyedCombineFn<K, VI, VA, VO>) transform.getFn();

			KvCoder<K, VI> inputCoder = (KvCoder<K, VI>) transform.getInput().getCoder();
			Coder<VA> accumulatorCoder =
					keyedCombineFn.getAccumulatorCoder(transform.getPipeline().getCoderRegistry(), inputCoder.getKeyCoder(), inputCoder.getValueCoder());

			// TODO: use this type information for grouping
			TypeInformation<KV<K, VI>> inputType = new KvCoderTypeInformation<>(inputCoder);
			TypeInformation<KV<K, VA>> partialReduceTypeInfo = new KvCoderTypeInformation<>(KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder));
			TypeInformation<KV<K, VO>> reduceTypeInfo = context.getTypeInfo(transform.getOutput());
			ExecutionConfig config = inputDataStream.getExecutionEnvironment().getConfig();

			// TODO: change to unclosed window
			if (!hasUnAppliedWindow){
				throw new UnsupportedOperationException("Cannot group unbound data flows.");
			} else {
				FlinkPartialWindowReduceFunction<K, VI, VA> partialReduceFunction = new FlinkPartialWindowReduceFunction<>(keyedCombineFn);
				FlinkWindowReduceFunction<K, VA, VO> reduceFunction = new FlinkWindowReduceFunction<>(keyedCombineFn);

				//Construct required windows
				GroupedDataStream<KV<K, VI>> groupedStream = new GroupedDataStream<>(inputDataStream,
						KeySelectorUtil.getSelectorForKeys(new Keys.ExpressionKeys<>(new String[]{"key"}, inputType), inputType, config));
				WindowedDataStream<KV<K,VI>> windowedStream = applyWindow(groupedStream);

				//Partially reduce to the intermediate format
				DiscretizedStream<KV<K, VA>> intermediateStream = windowedStream.mapWindow(partialReduceFunction, partialReduceTypeInfo)
						.name(partialOperatorName);

				//Reduce fully to output format VO
				DiscretizedStream<KV<K,VO>> outputStream = intermediateStream.mapWindow(reduceFunction, reduceTypeInfo)
						.name(totalOperatorName);

				context.setOutputDataStream(transform.getOutput(), outputStream.flatten());
			}
		}
	}

	private static class CombineGroupedValuesTranslator<K, VI, VA, VO> implements FlinkStreamingPipelineTranslator.TransformTranslator<Combine.GroupedValues<K, VI, VO>>{

		@Override
		public void translateNode(Combine.GroupedValues<K, VI, VO> transform, StreamingTranslationContext context) {
			DataStream<? extends KV<K, ? extends Iterable<VI>>> inputDataStream = context.getInputDataStream(transform.getInput());
			String partialOperatorName = transform.getName() + "-partial";
			String totalOperatorName = transform.getName() + "-total";

			@SuppressWarnings("unchecked")
			Combine.KeyedCombineFn<? super K, ? super VI, VA, VO> keyedCombineFn = (Combine.KeyedCombineFn<? super K, ? super VI, VA, VO>) transform.getFn();

			Coder<? extends KV<K, ? extends Iterable<VI>>> inputCoder = transform.getInput().getCoder();
			if (!(inputCoder instanceof KvCoder)) {
				throw new IllegalStateException(
						"Combine.GroupedValues requires its input to use KvCoder");
			}
			@SuppressWarnings({"unchecked", "rawtypes"})
			KvCoder<K, ? extends Iterable<VI>> kvCoder = (KvCoder) inputCoder;
			ExecutionConfig config = inputDataStream.getExecutionEnvironment().getConfig();

			TypeInformation<? extends KV<K, ? extends Iterable<VI>>> inputType = new CoderTypeInformation<>(inputCoder);

//			// TODO: use this type information for grouping
			TypeInformation<KV<K,VA>> partialReduceTypeInfo = new KvCoderTypeInformation<>(KvCoder.of(((KvCoder) inputCoder).getKeyCoder()
					, transform.getAccumulatorCoder()));
			TypeInformation<KV<K, VO>> reduceTypeInfo = context.getTypeInfo(transform.getOutput());

			FlinkPartialWindowIteratorReduceFunction<K, VI, VA> partialReduceFunction = new FlinkPartialWindowIteratorReduceFunction<>(keyedCombineFn);
			FlinkWindowReduceFunction<K, VA, VO> reduceFunction = new FlinkWindowReduceFunction<>(keyedCombineFn);

			//Construct required windows
			// TODO: Set this properly, problem is (? extends K) != (? extends K)
//			GroupedDataStream<KV<K, Iterable<VI>>> groupedStream = new GroupedDataStream<>(inputDataStream,
//					KeySelectorUtil.getSelectorForKeys(new Keys.ExpressionKeys<>(new String[]{"key"}, inputType), inputType, config));
			GroupedDataStream<KV<K, Iterable<VI>>> groupedStream = inputDataStream.groupBy(new KVKeySelector());
			WindowedDataStream<KV<K,Iterable<VI>>> windowedStream = applyWindow(groupedStream);

			//Partially reduce to the intermediate format
			DiscretizedStream<KV<K, VA>> intermediateStream = windowedStream.mapWindow(partialReduceFunction, partialReduceTypeInfo)
					.name(partialOperatorName);

			//Reduce fully to output format VO
			DiscretizedStream<KV<K,VO>> outputStream = intermediateStream.mapWindow(reduceFunction, reduceTypeInfo)
					.name(totalOperatorName);

			context.setOutputDataStream(transform.getOutput(), outputStream.flatten());
		}
	}

	private static class GroupAlsoByWindowsTranslator<K, V> implements
			FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.GroupAlsoByWindow<K, V>> {

		// not Flink's way, this would do the grouping by window
		// TODO: consider doing the windowing here
		@Override
		public void translateNode(GroupByKey.GroupAlsoByWindow<K, V> transform, StreamingTranslationContext context) {
			DataStream<KV<K, Iterable<WindowedValue<V>>>> inputDataStream = context.getInputDataStream(transform.getInput());
			TypeInformation<KV<K, Iterable<V>>> typeInformation = context.getTypeInfo(transform.getOutput());

			DataStream outputStream = inputDataStream.transform(transform.getName(), typeInformation,
					new StreamMap<>(inputDataStream.clean(new ToSimpleValue())));

			context.setOutputDataStream(transform.getOutput(), outputStream);
		}

		private class ToSimpleValue implements MapFunction<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>>{

			private List<V> nonWindowedValues;

			public ToSimpleValue(){
				nonWindowedValues = new ArrayList<>();
			}

			@Override
			public KV<K, Iterable<V>> map(KV<K, Iterable<WindowedValue<V>>> kv) throws Exception {
				nonWindowedValues.clear();
				for (WindowedValue<V> windowedValue : kv.getValue()){
					nonWindowedValues.add(windowedValue.getValue());
				}
				return KV.of(kv.getKey(), (Iterable<V>) nonWindowedValues);
			}
		}
	}

	private static class ReifyTimestampAndWindowsTranslator<K, V> implements
			FlinkStreamingPipelineTranslator.TransformTranslator<GroupByKey.ReifyTimestampsAndWindows<K, V>>{

		@Override
		public void translateNode(GroupByKey.ReifyTimestampsAndWindows<K, V> transform, final StreamingTranslationContext context) {
			DataStream<KV<K,V>> inputDataStream = context.getInputDataStream(transform.getInput());
			TypeInformation<KV<K, WindowedValue<V>>> typeInformation = context.getTypeInfo(transform.getOutput());

			DataStream<KV<K, WindowedValue<V>>> outputStream = inputDataStream.transform(transform.getName(), typeInformation,
					new StreamMap<>(inputDataStream.clean(new ToWindowedValue())));

			context.setOutputDataStream(transform.getOutput(), outputStream);
		}

		private class ToWindowedValue implements MapFunction<KV<K,V>, KV<K, WindowedValue<V>>>, Serializable{

			private Instant dummyInstant;
			private List<BoundedWindow> dummyList;

			public ToWindowedValue(){
				dummyInstant = Instant.now();
				dummyList = new ArrayList<>();
			}

			@Override
			public KV<K, WindowedValue<V>> map(KV<K, V> kv) throws Exception {
				return KV.of(kv.getKey(), WindowedValue.of(kv.getValue(), dummyInstant, dummyList));
			}
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

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	private static class KVKeySelector<K,V> implements KeySelector<KV<K,V>, K> {
		@Override
		public K getKey(KV<K,V> kv) throws Exception {
			return kv.getKey();
		}
	}

	private FlinkStreamingTransformTranslators() {}
}
