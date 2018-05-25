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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoDB;
import com.arangodb.internal.Host;
import com.arangodb.resilience.util.Instance;

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

	@Test
	public void leaderDown() {
		assertThat(arango.getVersion(), is(not(nullValue())));
		im.shudown(leader);
		assertThat(arango.getVersion(), is(not(nullValue())));
	}

}
