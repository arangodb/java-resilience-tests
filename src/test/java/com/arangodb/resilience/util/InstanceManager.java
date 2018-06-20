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

import static com.arangodb.resilience.util.EndpointUtils.host;
import static com.arangodb.resilience.util.EndpointUtils.port;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import com.arangodb.ArangoDB;
import com.arangodb.Protocol;
import com.arangodb.internal.Host;
import com.arangodb.velocypack.Type;
import com.arangodb.velocypack.VPack;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.RequestType;
import com.arangodb.velocystream.Response;

/**
 * @author Mark Vollmary
 *
 */
public class InstanceManager {

	private final ArangoDB connection;
	private final VPack vp;

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
		vp = new VPack.Builder().build();
	}

	private VPackSlice execute(final RequestType requestType, final String path) {
		return execute(requestType, path, null);
	}

	private VPackSlice execute(final RequestType requestType, final String path, final VPackSlice body) {
		return connection.execute(new Request(null, requestType, path).setBody(body)).getBody();
	}

	private Collection<Instance> deserialize(final VPackSlice vpack) {
		return vpack == null ? Collections.emptyList() : vp.deserialize(vpack, new Type<Collection<Instance>>() {
		}.getType());
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

	public Collection<Instance> startAgency() {
		return deserialize(execute(RequestType.POST, "/agency"));
	}

	public Collection<Instance> startSingleServer(final int num) {
		final Response response = connection
				.execute(new Request(null, RequestType.POST, "/single").putQueryParam("num", num));
		return deserialize(response.getBody());
	}

	public Collection<Instance> coordinators() {
		return deserialize(execute(RequestType.GET, "/instance/coordinators"));
	}

	public Collection<Instance> singleServers() {
		return deserialize(execute(RequestType.GET, "/instance/single"));
	}

	public void waitForAllInstances() {
		execute(RequestType.HEAD, "/instance");
	}

	public void waitForInstance(final String name) {
		execute(RequestType.HEAD, "/instance/" + name);
	}

	public Instance getReplicationLeader() {
		return vp.deserialize(execute(RequestType.GET, "/replication/leader"), Instance.class);
	}

	public String getReplicationLeaderId() {
		return execute(RequestType.GET, "/replication/leader/id").get("uuid").getAsString();
	}

	public void waitForReplicationLeader() {
		waitForReplicationLeader(null);
	}

	public void waitForReplicationLeader(final String uuid) {
		final Request request = new Request(null, RequestType.HEAD, "/replication/leader");
		if (uuid != null) {
			request.putQueryParam("ignore", uuid);
		}
		connection.execute(request);
	}

	public boolean isRunning(final Instance instance) {
		return execute(RequestType.GET, "/instance/" + instance.getName()).get("status").getAsString()
				.equals("RUNNING");
	}

	public void shutdown(final Instance instance) {
		shutdown(instance, false);
	}

	public void kill(final Instance instance) {
		shutdown(instance, true);
	}

	public void shutdown(final Instance instance, final boolean kill) {
		connection.execute(
			new Request(null, RequestType.DELETE, "/instance/" + instance.getName()).putQueryParam("kill", kill));
	}

	public void restart(final Instance instance) {
		execute(RequestType.POST, "/instance/" + instance.getName());
	}

	public void cleanup() {
		execute(RequestType.DELETE, "/");
	}

	public void shutdown() {
		connection.shutdown();
	}

}
