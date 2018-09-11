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

import static com.arangodb.resilience.util.EndpointUtils.host;
import static com.arangodb.resilience.util.EndpointUtils.port;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoDB;
import com.arangodb.internal.net.HostDescription;
import com.arangodb.internal.util.RequestUtils;
import com.arangodb.resilience.util.Instance;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.RequestType;

/**
 * @author Mark Vollmary
 *
 */
public abstract class BaseFailoverTest extends BaseTest {

	private Instance leader;
	private String uuid;

	@Before
	public void setup() {
		im.startAgency();
		im.startSingleServer(2);
		im.waitForAllInstances();
		im.waitForReplicationLeader();
		uuid = im.getReplicationLeaderId();
		leader = im.getReplicationLeader();
		final ArangoDB.Builder builder = new ArangoDB.Builder();
		configure(builder, new HostDescription(host(leader.getEndpoint()), port(leader.getEndpoint())));
		arango = builder.build();
	}

	protected abstract void configure(final ArangoDB.Builder builder, final HostDescription leader);

	@After
	public void teardown() {
		arango.shutdown();
		im.cleanup();
	}

	protected String serverId() {
		final VPackSlice execute = execute(RequestType.GET, "/_api/replication/server-id");
		return execute.get("serverId").toString();
	}

	protected String serverIdDirty() {
		final VPackSlice execute = executeDirty(RequestType.GET, "/_api/replication/server-id", null);
		return execute.get("serverId").toString();
	}

	protected VPackSlice executeDirty(final RequestType requestType, final String path, final VPackSlice body) {
		return arango.execute(new Request("_system", requestType, path).setBody(body)
				.putHeaderParam(RequestUtils.HEADER_ALLOW_DIRTY_READ, "true")).getBody();
	}

	protected Map<String, String> responseHeader() {
		return arango.execute(new Request("_system", RequestType.GET, "/_api/version")).getMeta();
	}

	@Test
	public void leaderDown() {
		final String leaderId = serverId();
		assertThat(leaderId, is(not(nullValue())));
		assertThat(responseHeader().containsKey("X-Arango-Endpoint"), is(false));
		im.kill(leader);
		im.waitForReplicationLeader(uuid);
		try {
			Thread.sleep(3000); // agency plan is upgraded but new leader still responses with header
								// "X-Arango-Endpoint"
		} catch (final InterruptedException e) {
		}
		final String newLeaderId = serverId();
		assertThat(newLeaderId, is(not(nullValue())));
		assertThat(newLeaderId, is(not(leaderId)));
		assertThat(responseHeader().containsKey("X-Arango-Endpoint"), is(false));
	}

	@Test
	public void dirtyRead() {
		final String leader = serverIdDirty();
		assertThat(leader, is(not(nullValue())));
		assertThat(serverIdDirty(), is(not(leader)));

		assertThat(serverId(), is(leader));
		assertThat(serverId(), is(leader));
	}

}
