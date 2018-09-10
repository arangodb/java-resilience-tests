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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.arangodb.ArangoDB.Builder;
import com.arangodb.internal.net.HostDescription;
import com.arangodb.resilience.util.Instance;

/**
 * @author Mark Vollmary
 *
 */
public class LoadBalancingConnectionTtlVstTest extends BaseLoadBalancingTest {

	@Override
	protected void configure(final Builder builder, final HostDescription endpoint) {
		builder.host(endpoint.getHost(), endpoint.getPort());
		builder.acquireHostList(true);
		builder.connectionTtl(3 * 1000L);
	}

	@Test
	public void coordinatorUpAgainTtlSelfBalance() throws InterruptedException {
		final List<String> serverIds = new ArrayList<>();
		Stream.iterate(0, i -> i + 1).limit(NUM_COORDINATORS).forEach(i -> {
			final String serverId = serverId();
			// assert that every call goes to a different coordinator
			assertThat(serverIds, not(hasItem(serverId)));
			serverIds.add(serverId);
		});
		final Instance coordinator = im.coordinators().stream().findFirst().get();
		im.shutdown(coordinator);
		assertThat(im.isRunning(coordinator), is(false));
		// perform 3 operations to be sure that all connections are used
		Stream.iterate(0, i -> i + 1).limit(NUM_COORDINATORS).map(i -> arango.getVersion());
		Thread.sleep(4 * 1000L); // wait until all connection ttls are expired
		im.restart(coordinator);
		assertThat(im.isRunning(coordinator), is(true));
		final List<String> thirdRun = Stream.iterate(0, i -> i + 1).limit(NUM_COORDINATORS).map(i -> serverId())
				.collect(Collectors.toList());
		assertThat(serverIds, hasItems(thirdRun.toArray(new String[] {})));
		// assert that the thridRun includes all 3 coordinators
		assertThat(serverIds.stream().filter(i -> !thirdRun.contains(i)).count(), is(0L));
	}

}
