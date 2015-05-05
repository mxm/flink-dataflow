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
package com.dataartisans.flink.dataflow.runner;

import com.dataartisans.flink.dataflow.streaming.translation.FlinkStreamingPipelineTranslator;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner} that executes the operations in the
 * pipeline by first translating them to a Flink Plan and then executing them either locally
 * or on a Flink cluster, depending on the configuration.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner}.
 */
public class FlinkStreamingPipelineRunner extends FlinkPipelineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamingPipelineRunner.class);

	/**
	 * The Flink execution environment. This is instantiated to either a
	 * {@link org.apache.flink.api.java.CollectionEnvironment},
	 * a {@link org.apache.flink.api.java.LocalEnvironment} or
	 * a {@link org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration
	 * options.
	 */
	private final StreamExecutionEnvironment flinkEnv;

	/** Translator for this FlinkPipelineRunner, based on options. */
	private final FlinkStreamingPipelineTranslator translator;


	FlinkStreamingPipelineRunner(FlinkPipelineOptions options) {
		super(options);

		this.flinkEnv = createExecutionEnvironment(options);
		//for testing purposes
//		flinkEnv.getStreamGraph().setChaining(false); // does not help
		flinkEnv.setParallelism(1);

		this.translator = new FlinkStreamingPipelineTranslator(flinkEnv, options);
	}

	/**
	 * Create Flink {@link org.apache.flink.api.java.ExecutionEnvironment} depending
	 * on the options.
	 */
	private StreamExecutionEnvironment createExecutionEnvironment(FlinkPipelineOptions options) {
		String masterUrl = options.getFlinkMaster();


		if (masterUrl.equals("[local]")) {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			if (options.getParallelism() != -1) {
				env.setParallelism(options.getParallelism());
			}
			return env;
		} else if (masterUrl.equals("[auto]")) {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			if (options.getParallelism() != -1) {
				env.setParallelism(options.getParallelism());
			}
			return env;
		} else if (masterUrl.matches(".*:\\d*")) {
			String[] parts = masterUrl.split(":");
			List<String> stagingFiles = options.getFilesToStage();
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(parts[0],
					Integer.parseInt(parts[1]),
					stagingFiles.toArray(new String[stagingFiles.size()]));
			if (options.getParallelism() != -1) {
				env.setParallelism(options.getParallelism());
			}
			return env;
		} else {
			LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			if (options.getParallelism() != -1) {
				env.setParallelism(options.getParallelism());
			}
			return env;
		}
	}

	@Override
	public FlinkRunnerResult run(Pipeline pipeline) {
		LOG.info("Executing pipeline using FlinkPipelineRunner.");
		
		LOG.info("Translating pipeline to Flink program.");
		
		translator.translate(pipeline);
		
		LOG.info("Starting execution of Flink program.");
		
		JobExecutionResult result = null;
		try {
			result = flinkEnv.execute();
		}
		catch (Exception e) {
			LOG.error("Pipeline execution failed", e);
			throw new RuntimeException("Pipeline execution failed", e);
		}
		
		LOG.info("Execution finished in {} msecs", result.getNetRuntime());
		
		Map<String, Object> accumulators = result.getAllAccumulatorResults();
		if (accumulators != null && !accumulators.isEmpty()) {
			LOG.info("Final aggregator values:");
			
			for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
				LOG.info("{} : {}", entry.getKey(), entry.getValue());
			}
		}

		return new FlinkRunnerResult(accumulators, result.getNetRuntime());
	}

	/**
	 * Constructs a runner with default properties for testing.
	 *
	 * @return The newly created runner.
	 */
	public static FlinkStreamingPipelineRunner createForTest() {
		FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
		// we use [auto] for testing since this will make it pick up the Testing
		// ExecutionEnvironment
		options.setFlinkMaster("[auto]");
		return new FlinkStreamingPipelineRunner(options);
	}

	@Override
	public <Output extends POutput, Input extends PInput> Output apply(
			PTransform<Input, Output> transform, Input input) {
		return super.apply(transform, input);
	}

	/////////////////////////////////////////////////////////////////////////////

	@Override
	public String toString() { return "DataflowPipelineRunner#" + hashCode(); }

	/**
	 * Attempts to detect all the resources the class loader has access to. This does not recurse
	 * to class loader parents stopping it from pulling in resources from the system class loader.
	 *
	 * @param classLoader The URLClassLoader to use to detect resources to stage.
	 * @throws IllegalArgumentException  If either the class loader is not a URLClassLoader or one
	 * of the resources the class loader exposes is not a file resource.
	 * @return A list of absolute paths to the resources the class loader uses.
	 */
	protected static List<String> detectClassPathResourcesToStage(ClassLoader classLoader) {
		if (!(classLoader instanceof URLClassLoader)) {
			String message = String.format("Unable to use ClassLoader to detect classpath elements. "
					+ "Current ClassLoader is %s, only URLClassLoaders are supported.", classLoader);
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}

		List<String> files = new ArrayList<>();
		for (URL url : ((URLClassLoader) classLoader).getURLs()) {
			try {
				files.add(new File(url.toURI()).getAbsolutePath());
			} catch (IllegalArgumentException | URISyntaxException e) {
				String message = String.format("Unable to convert url (%s) to file.", url);
				LOG.error(message);
				throw new IllegalArgumentException(message, e);
			}
		}
		return files;
	}
}
