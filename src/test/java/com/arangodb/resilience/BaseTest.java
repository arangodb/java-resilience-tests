/*
 * DISCLAIMER
 *
 * Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package com.arangodb.resilience;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.arangodb.ArangoDB;
import com.arangodb.resilience.util.InstanceManager;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.RequestType;

/**
 * @author Mark Vollmary
 *
 */
public abstract class BaseTest {

	protected static InstanceManager im;
	protected ArangoDB arango;

	@BeforeClass
	public static void initInstanceManager() {
		im = new InstanceManager();
	}

	@AfterClass
	public static void shutdownInstanceManager() {
		im.shutdown();
	}

	protected VPackSlice execute(final RequestType requestType, final String path) {
		return execute(requestType, path, null);
	}

	protected VPackSlice execute(final RequestType requestType, final String path, final VPackSlice body) {
		return arango.execute(new Request("_system", requestType, path).setBody(body)).getBody();
	}
}
