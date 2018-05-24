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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDB.Builder;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.internal.Host;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.velocystream.RequestType;

/**
 * @author Mark Vollmary
 *
 */
public abstract class BaseLoadBalancingTest extends BaseTest {

	private static final int NUM_COORDINATORS = 3;

	@Before
	public void setup() {
		final Host endpoint = im.startCluster(1, NUM_COORDINATORS, 2);
		final Builder builder = new ArangoDB.Builder() //
				.maxConnections(NUM_COORDINATORS) //
				.loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN);
		configure(builder, endpoint);
		arango = builder.build();
	}

	protected abstract void configure(final ArangoDB.Builder builder, final Host endpoint);

	@After
	public void teardown() {
		arango.shutdown();
		im.cleanup();
	}

	private String serverId() {
		return execute(RequestType.GET, "/_admin/status").get("serverInfo").get("serverId").toString();
	}

	@Test
	public void loadBalance() {
		final List<String> serverIds = new ArrayList<>();
		Stream.iterate(0, i -> i + 1).limit(NUM_COORDINATORS).forEach(i -> {
			final String serverId = serverId();
			assertThat(serverIds, not(hasItem(serverId)));
			serverIds.add(serverId);
		});
		Stream.iterate(0, i -> i + 1).limit(NUM_COORDINATORS).forEach(i -> {
			assertThat(serverIds.get(i), is(serverId()));
		});
	}

	@Test
	public void cursorStickiness() {
		final ArangoCursor<Integer> cursor = arango.db().query("FOR i IN 1..2 RETURN i", null,
			new AqlQueryOptions().batchSize(1), int.class);
		final long count = StreamSupport.stream(cursor.spliterator(), false).count();
		assertThat(count, is(2L));
	}

}
