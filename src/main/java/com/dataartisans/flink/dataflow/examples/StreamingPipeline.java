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
package com.dataartisans.flink.dataflow.examples;

import com.dataartisans.flink.dataflow.FlinkPipelineOptions;
import com.dataartisans.flink.dataflow.streaming.FlinkStreamingPipelineRunner;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import org.joda.time.Duration;

public class StreamingPipeline {

	public static class Tokenizer extends DoFn<String, String> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			// Split the line into words.
			String[] words = c.element().toLowerCase().split("[^a-zA-Z']+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}

	/** A DoFn that converts a Word and Count into a printable string. */
	static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
		private static final long serialVersionUID = 0;

		@Override
		public void processElement(ProcessContext c) {
			String output = "Element: " + c.element().getKey()
					+ " Value: " + c.element().getValue();
			c.output(output);
		}
	}

	/**
	 * Options supported by {@link StreamingPipeline}.
	 * <p>
	 * Inherits standard configuration options.
	 */
	public static interface Options extends PipelineOptions, FlinkPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
		String getInput();
		void setInput(String value);

		@Description("Path of the file to write to")
		String getOutput();
		void setOutput(String value);

		/**
		 * By default (numShards == 0), the system will choose the shard count.
		 * Most programs will not need this option.
		 */
		@Description("Number of output shards (0 if the system should choose automatically)")
		int getNumShards();
		void setNumShards(int value);
	}
	
	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
		options.setRunner(FlinkStreamingPipelineRunner.class);

		options.setStreaming(true);

		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(ParDo.of(new Tokenizer()))
				.apply(Window.<String>into(FixedWindows.of(Duration.millis(1))))
				.apply(Count.<String>perElement())
				.apply(ParDo.of(new FormatCountsFn()))
				.apply(TextIO.Write.named("WriteCounts")
						.to(options.getOutput())
						.withNumShards(options.getNumShards()));

		p.run();
	}

}
