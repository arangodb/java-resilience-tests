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

package com.arangodb.resilience.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.arangodb.ArangoDB;
import com.arangodb.Protocol;
import com.arangodb.internal.Host;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.RequestType;

/**
 * @author Mark Vollmary
 *
 */
public class InstanceManager {

	private final ArangoDB connection;

	public InstanceManager() {
		super();
		try {
			final Properties properties = new Properties();
			properties.load(InstanceManager.class.getResourceAsStream("/aim.properties"));
			final String endpoint = properties.getProperty("aim.endpoint", "127.0.0.1:9000");
			final Host host = new Host(host(endpoint), port(endpoint));
			connection = new ArangoDB.Builder().useProtocol(Protocol.HTTP_JSON).host(host.getHost(), host.getPort())
					.user(null).build();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static String host(final String endpoint) {
		return endpoint.replace("tcp://", "").replace("http://", "").split(":")[0];
	}

	private static int port(final String endpoint) {
		final String[] split = endpoint.split(":");
		return Integer.valueOf(split[split.length - 1]);
	}

	protected VPackSlice execute(final RequestType requestType, final String path) {
		return execute(requestType, path, null);
	}

	protected VPackSlice execute(final RequestType requestType, final String path, final VPackSlice body) {
		return connection.execute(new Request(null, requestType, path).setBody(body)).getBody();
	}

	public Host startCluster(final int numAgents, final int numCoordinators, final int numDbServeres) {
		final VPackSlice body = new VPackBuilder() //
				.add(ValueType.OBJECT) //
				.add("numAgents", numAgents) //
				.add("numCoordinators", numCoordinators) //
				.add("numDbServeres", numDbServeres) //
				.close().slice();
		final String endpoint = execute(RequestType.POST, "/cluster", body).get("endpoint").getAsString();
		return new Host(host(endpoint), port(endpoint));
	}

	public Collection<Instance> coordinators() {
		final Iterator<VPackSlice> it = execute(RequestType.GET, "/cluster/coordinators").arrayIterator();
		final Iterable<VPackSlice> iterable = () -> it;
		return StreamSupport.stream(iterable.spliterator(), false).map(c -> {
			final String e = c.get("endpoint").getAsString();
			final Host endpoint = new Host(host(e), port(e));
			return new Instance(c.get("name").getAsString(), endpoint);
		}).collect(Collectors.toList());
	}

	public boolean isRunning(final Instance instance) {
		return execute(RequestType.GET, "/instance/" + instance.getName()).get("status").getAsString()
				.equals("RUNNING");
	}

	public void shudown(final Instance instance) {
		execute(RequestType.DELETE, "/instance/" + instance.getName());
	}

	public void restart(final Instance instance) {
		execute(RequestType.PATCH, "/instance/" + instance.getName());
	}

	public void cleanup() {
		execute(RequestType.DELETE, "/");
	}

	public void shutdown() {
		connection.shutdown();
	}

}
