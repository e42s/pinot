/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.OutOfCapacityError;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceManager;
import com.linkedin.pinot.core.query.scheduler.SchedulerPriorityQueue;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import com.linkedin.pinot.core.query.scheduler.TableBasedGroupMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Priority queues of scheduler groups that determines query priority based on tokens
 *
 * This is a multi-level query scheduling queue with each sublevel maintaining a waitlist of
 * queries for the group. The priority between groups is determined based on the available
 * number of tokens. If two groups have same number of tokens then the group with lower
 * resource utilization is selected first. Oldest query from the winning SchedulerGroup
 * is selected for execution.
 *
 */
public class TokenPriorityQueue implements SchedulerPriorityQueue {

  private static Logger LOGGER = LoggerFactory.getLogger(TokenPriorityQueue.class);
  public static final String TOKENS_PER_MS_KEY = "tokens_per_ms";
  public static final String TOKEN_LIFETIME_MS_KEY = "token_lifetime_ms";
  private static final String QUERY_DEADLINE_SECONDS_KEY = "query_deadline_seconds";
  private static final String MAX_PENDING_PER_GROUP_KEY = "max_pending_per_group";

  private static final int DEFAULT_TOKEN_LIFETIME_MS = 100;

  private final Map<String, SchedulerTokenGroup> schedulerGroups = new HashMap<>();
  private final Lock queueLock = new ReentrantLock();
  private final Condition queryReaderCondition = queueLock.newCondition();
  private final int tokenLifetimeMs;
  private final int tokensPerMs;
  private final int maxPendingPerGroup;
  private final ResourceManager resourceManager;
  private final TableBasedGroupMapper groupSelector;
  private final int queryDeadlineMillis;

  public TokenPriorityQueue(@Nonnull Configuration config, @Nonnull ResourceManager resourceManager) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(resourceManager);

    // max available tokens per millisecond equals number of threads (total execution capacity)
    // we are over provisioning tokens here because its better to keep pipe full rather than empty
    int maxTokensPerMs = resourceManager.getNumQueryRunnerThreads() + resourceManager.getNumQueryWorkerThreads();
    tokensPerMs = config.getInt(TOKENS_PER_MS_KEY, maxTokensPerMs);
    tokenLifetimeMs = config.getInt(TOKEN_LIFETIME_MS_KEY, DEFAULT_TOKEN_LIFETIME_MS);
    queryDeadlineMillis = config.getInt(QUERY_DEADLINE_SECONDS_KEY, 30) * 1000;
    maxPendingPerGroup = config.getInt(MAX_PENDING_PER_GROUP_KEY, 10);
    this.resourceManager = resourceManager;
    // TODO: This should be wired in based on configuration in future
    this.groupSelector = new TableBasedGroupMapper();
  }

  @Override
  public void put(@Nonnull SchedulerQueryContext query) throws OutOfCapacityError {
    Preconditions.checkNotNull(query);
    queueLock.lock();
    String groupName = groupSelector.getSchedulerGroupName(query);
    try {
      SchedulerTokenGroup groupContext = getOrCreateGroupContext(groupName);
      checkGroupHasCapacity(groupContext);
      query.setSchedulerGroupContext(groupContext);
      groupContext.addLast(query);
      queryReaderCondition.signal();
    } finally {
      queueLock.unlock();
    }
  }

  private void checkGroupHasCapacity(SchedulerTokenGroup groupContext) throws OutOfCapacityError {
    if (groupContext.numPending() < maxPendingPerGroup &&
        groupContext.totalReservedThreads() < resourceManager.getTableThreadsHardLimit()) {
      return;
    }
    throw new OutOfCapacityError(
        String.format("SchedulerGroup %s is out of capacity. numPending: %d, maxPending: %d, reservedThreads: %d threadsHardLimit: %d",
            groupContext.name(),
            groupContext.numPending(), maxPendingPerGroup,
            groupContext.totalReservedThreads(), resourceManager.getTableThreadsHardLimit()));
  }

  public SchedulerTokenGroup getOrCreateGroupContext(String groupName) {
    SchedulerTokenGroup groupContext = schedulerGroups.get(groupName);
    if (groupContext == null) {
      groupContext = new SchedulerTokenGroup(groupName, tokensPerMs, tokenLifetimeMs);
      schedulerGroups.put(groupName, groupContext);
    }
    return groupContext;
  }

  /**
   * Blocking call to read the next query in order of priority
   * @return
   */
  @Override
  public @Nonnull SchedulerQueryContext take() {
    queueLock.lock();
    try {
      while (true) {
        SchedulerQueryContext schedulerQueryContext;
        while ( (schedulerQueryContext = takeNextInternal()) == null) {
          try {
            queryReaderCondition.await(200, TimeUnit.MICROSECONDS);
          } catch (InterruptedException e) {
            // continue
          }
        }
        return schedulerQueryContext;
      }
    } finally {
      queueLock.unlock();
    }
  }

  private SchedulerQueryContext takeNextInternal() {
    int selectedTokens = Integer.MIN_VALUE;
    SchedulerTokenGroup selectedGroup = null;
    long startTime = System.nanoTime();
    StringBuilder sb = new StringBuilder("SchedulerInfo:");
    for (Map.Entry<String, SchedulerTokenGroup> groupInfoEntry : schedulerGroups.entrySet()) {
      SchedulerTokenGroup currentGroup = groupInfoEntry.getValue();
      sb.append(String.format(" {%s:[%d,%d,%d,%d,%d]},", currentGroup.name(),
          currentGroup.getAvailableTokens(),
          currentGroup.numPending(),
          currentGroup.numRunning(),
          currentGroup.getThreadsInUse(),
          currentGroup.totalReservedThreads()));
      if (currentGroup.isEmpty() ||
          currentGroup.getAvailableTokens() < selectedTokens ||
          !resourceManager.canSchedule(currentGroup)) {
        continue;
      }

      if (selectedGroup == null) {
        selectedGroup = currentGroup;
        continue;
      }
      currentGroup.trimExpired(queryDeadlineMillis);
      // Preconditions:
      // a. currentGroupResources <= hardLimit
      // b. selectedGroupResources <= hardLimit
      // We prefer group with higher tokens but with resource limits.
      // If current groupTokens are greater than selectedTokens then we choose current
      // group over selectedGroup if
      // a. current group is using less than softLimit resources
      // b. if softLimit < currentGroupResources <= hardLimit then
      //     i. choose currentGroup if softLimit <= selectedGroup <= hardLimit
      //     ii. continue with selectedGroup otherwise
      int comparison = currentGroup.compareTo(selectedGroup);
      if (comparison < 0) {
        continue;
      }
      if (comparison >= 0) {
        if (currentGroup.totalReservedThreads() < resourceManager.getTableThreadsSoftLimit() ||
            (selectedGroup.totalReservedThreads() > resourceManager.getTableThreadsSoftLimit() &&
            currentGroup.totalReservedThreads() < selectedGroup.totalReservedThreads())) {
          selectedGroup = currentGroup;
        }
      }
    }
    SchedulerQueryContext query = null;
    if (selectedGroup != null) {
      ServerQueryRequest queryRequest = selectedGroup.peekFirst().getQueryRequest();
      sb.append(String.format(" Winner: %s: [%d,%d,%d,%d]", selectedGroup.name(),
          queryRequest.getTimerContext().getQueryArrivalTimeMs(),
          queryRequest.getInstanceRequest().getRequestId(),
          queryRequest.getInstanceRequest().getSearchSegments().size(),
          startTime));
      query = selectedGroup.removeFirst();
    }
    LOGGER.info(sb.toString());
    long endTime = System.nanoTime();
    return query;
  }
}

