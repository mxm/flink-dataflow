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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to a Flink Plan and then executing them either locally
 * or on a Flink cluster, depending on the configuration.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner}.
 */
public abstract class FlinkPipelineRunner extends PipelineRunner<FlinkRunnerResult> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineRunner.class);

	/** Provided options. */
	protected final FlinkPipelineOptions options;

	/**
	 * Construct a runner from the provided options.
	 *
	 * @param options Properties which configure the runner.
	 * @return The newly created runner.
	 */
	public static FlinkPipelineRunner fromOptions(PipelineOptions options) {
		FlinkPipelineOptions flinkOptions =
				PipelineOptionsValidator.validate(FlinkPipelineOptions.class, options);
		ArrayList<String> missing = new ArrayList<>();

		if (flinkOptions.getAppName() == null) {
			missing.add("appName");
		}
		if (missing.size() > 0) {
			throw new IllegalArgumentException(
					"Missing required values: " + Joiner.on(',').join(missing));
		}

		if (flinkOptions.getFilesToStage() == null) {
			flinkOptions.setFilesToStage(detectClassPathResourcesToStage(
					DataflowPipelineRunner.class.getClassLoader()));
			LOG.info("PipelineOptions.filesToStage was not specified. "
							+ "Defaulting to files from the classpath: will stage {} files. "
							+ "Enable logging at DEBUG level to see which files will be staged.",
					flinkOptions.getFilesToStage().size());
			LOG.debug("Classpath elements: {}", flinkOptions.getFilesToStage());
		}

		// Verify jobName according to service requirements.
		String jobName = flinkOptions.getJobName().toLowerCase();
		Preconditions.checkArgument(jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"), "JobName invalid; " +
				"the name must consist of only the characters " + "[-a-z0-9], starting with a letter " +
				"and ending with a letter " + "or number");
		Preconditions.checkArgument(jobName.length() <= 40,
				"JobName too long; must be no more than 40 characters in length");

		// Set Flink Master to [auto] if no option was specified.
		if (flinkOptions.getFlinkMaster() == null) {
			flinkOptions.setFlinkMaster("[auto]");
		}

		return instantiateRunner(options.as(FlinkPipelineOptions.class));
	}

	private static FlinkPipelineRunner instantiateRunner(FlinkPipelineOptions options) {

		if (options.isStreaming()) {
			return new FlinkStreamingPipelineRunner(options);
		} else {
			return new FlinkBatchPipelineRunner(options);
		}
	}

	protected FlinkPipelineRunner(FlinkPipelineOptions options){
		this.options = options;
	}

	/**
	 * For testing.
	 */
	public FlinkPipelineOptions getPipelineOptions() {
		return options;
	}


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
