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
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.ReadSource;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translators for transforming
 * Dataflow {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s to
 * Flink {@link org.apache.flink.api.java.DataSet}s
 */
public class FlinkStreamingTransformTranslators {

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

		//TRANSLATORS.put(BigQueryIO.Read.Bound.class, null);
		//TRANSLATORS.put(BigQueryIO.Write.Bound.class, null);

		//TRANSLATORS.put(DatastoreIO.Sink.class, null);

		//TRANSLATORS.put(PubsubIO.Read.Bound.class, null);
		//TRANSLATORS.put(PubsubIO.Write.Bound.class, null);

		TRANSLATORS.put(ReadSource.Bound.class, new ReadSourceTranslator());

		TRANSLATORS.put(TextIO.Read.Bound.class, new TextIOReadTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteTranslator());
	}


	public static FlinkStreamingPipelineTranslator.TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}

	private static class ReadSourceTranslator<T> implements FlinkStreamingPipelineTranslator.TransformTranslator<ReadSource.Bound<T>> {

		@Override
		public void translateNode(ReadSource.Bound<T> transform, StreamingTranslationContext context) {
			String name = transform.getName();
			Source<T> source = transform.getSource();
			Coder<T> coder = transform.getOutput().getCoder();

			TypeInformation<T> typeInformation = context.getTypeInfo(transform.getOutput());

			// TODO: Add DataStreamSource accordingly
//			DataStreamSource<T> dataSource = new DataStreamSource<>(context.getExecutionEnvironment(), new SourceInputFormat<>(source, context.getPipelineOptions(), coder), typeInformation, name);
//
//			context.setOutput(transform.getOutput(), dataSource);
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

			TypeInformation<String> typeInformation = context.getTypeInfo(transform.getOutput());

			// TODO: Add DataStreamSource accordingly
//			DataSource<String> source = new DataSource<>(context.getExecutionEnvironment(), new TextInputFormat(new Path(path)), typeInformation, name);
//
//			context.setOutputDataStream(transform.getOutput(), source);
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
//			DataSet<IN> inputDataSet = context.getInputDataSet(transform.getInput());
//
//			final DoFn<IN, OUT> doFn = transform.getFn();
//
//			if (doFn instanceof DoFn.RequiresKeyedState) {
//				LOG.error("Flink Batch Execution does not support Keyed State.");
//			}
//
//			TypeInformation<OUT> typeInformation = context.getTypeInfo(transform.getOutput());
//
//			FlinkDoFnFunction<IN, OUT> doFnWrapper = new FlinkDoFnFunction<>(doFn, context.getPipelineOptions());
//			MapPartitionOperator<IN, OUT> outputDataSet = new MapPartitionOperator<>(inputDataSet, typeInformation, doFnWrapper, transform.getName());
//
//			transformSideInputs(transform.getSideInputs(), outputDataSet, context);
//
//			context.setOutputDataSet(transform.getOutput(), outputDataSet);
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
