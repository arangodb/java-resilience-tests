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
import com.arangodb.internal.Host;
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

	@Before
	public void setup() {
		im.startAgency();
		im.startSingleServer(2);
		im.waitForAllInstances();
		im.waitForReplicationLeader();
		leader = im.getReplicationLeader();
		final ArangoDB.Builder builder = new ArangoDB.Builder();
		configure(builder, new Host(host(leader.getEndpoint()), port(leader.getEndpoint())));
		arango = builder.build();
	}

	protected abstract void configure(final ArangoDB.Builder builder, final Host leader);

	@After
	public void teardown() {
		arango.shutdown();
		im.cleanup();
	}

	protected String serverId() {
		final VPackSlice execute = execute(RequestType.GET, "/_api/replication/server-id");
		return execute.get("serverId").toString();
	}

	protected Map<String, String> responseHeader() {
		return arango.execute(new Request("_system", RequestType.GET, "/_api/version")).getMeta();
	}

	@Test
	public void leaderDown() {
		final String leaderId = serverId();
		assertThat(leaderId, is(not(nullValue())));
		assertThat(responseHeader().containsKey("X-Arango-Endpoint"), is(false));
		im.shudown(leader);
		final String newLeaderId = serverId();
		assertThat(newLeaderId, is(not(nullValue())));
		assertThat(newLeaderId, is(not(leaderId)));
		assertThat(responseHeader().containsKey("X-Arango-Endpoint"), is(false));
	}

}
