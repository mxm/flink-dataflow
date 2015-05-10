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
package com.dataartisans.flink.dataflow.util;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;

import java.io.IOException;

// Implementation is copied from https://cloud.google.com/pubsub/configure,
// licensed Apache 2.0

/**
 * Create a Pubsub client using portable credentials.
 */
public class PortableConfiguration {

	// Default factory method.
	public static Pubsub createPubsubClient() throws IOException {
		return createPubsubClient(Utils.getDefaultTransport(),
				Utils.getDefaultJsonFactory());
	}

	// A factory method that allows you to use your own HttpTransport
	// and JsonFactory.
	public static Pubsub createPubsubClient(HttpTransport httpTransport,
											JsonFactory jsonFactory) throws IOException {
		Preconditions.checkNotNull(httpTransport);
		Preconditions.checkNotNull(jsonFactory);
		GoogleCredential credential = GoogleCredential.getApplicationDefault(
				httpTransport, jsonFactory);
		// In some cases, you need to add the scope explicitly.
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(PubsubScopes.all());
		}
		// Please use custom HttpRequestInitializer for automatic
		// retry upon failures.  We provide a simple reference
		// implementation in the "Retry Handling" section.
		HttpRequestInitializer initializer =
				new RetryHttpInitializerWrapper(credential);
		return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
				.build();
	}
}
