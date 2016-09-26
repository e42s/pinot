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

import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroup;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Scheduler group that manages accounting based on the number of tokens.
 *
 * Each SchedulerGroup is allotted a set of token periodically. Token represents
 * a unit of thread wall clock time. Tokens are deducted from group for each unit
 * of time per thread that this group uses. New batch of tokens are allotted periodically
 * by applying linear decay. Linear decay memorizes resource utilization in the previous
 * time quantum penalizing heavy users. This is important to give fair chance to low qps
 * workloads.
 */
public class SchedulerTokenGroup implements SchedulerGroup {

  private final String schedGroupName;
  private final int tokenLifetimeMs;
  private final int numTokensPerMs;

  private int availableTokens;
  private long lastUpdateTimeMs;
  private long lastTokenTimeMs;
  private int threadsInUse;
  private final ConcurrentLinkedQueue<SchedulerQueryContext> pendingQueries = new ConcurrentLinkedQueue<>();

  private final Lock tokenLock = new ReentrantLock();
  private int runningQueries = 0;
  private static final double ALPHA = 0.80;
  private AtomicInteger reservedThreads = new AtomicInteger(0);

  SchedulerTokenGroup(String schedGroupName, int numTokensPerMs, int tokenLifetimeMs) {
    this.schedGroupName = schedGroupName;
    this.numTokensPerMs = numTokensPerMs;
    this.tokenLifetimeMs = tokenLifetimeMs;
    lastUpdateTimeMs = System.currentTimeMillis();
    availableTokens = numTokensPerMs * tokenLifetimeMs;
    lastTokenTimeMs = lastUpdateTimeMs;
  }

  public String name() {
    return schedGroupName;
  }

  @Override
  public int numRunning() {
    return runningQueries;
  }

  @Override
  public void addLast(SchedulerQueryContext query) {
    pendingQueries.add(query);
  }

  @Override
  public SchedulerQueryContext peekFirst() {
    return pendingQueries.peek();
  }

  @Override
  public SchedulerQueryContext removeFirst() {
    return pendingQueries.poll();
  }

  @Override
  public void trimExpired(long deadlineMillis) {
    Iterator<SchedulerQueryContext> iter = pendingQueries.iterator();
    while (iter.hasNext()) {
      SchedulerQueryContext next = iter.next();
      if (next.getArrivalTimeMs() < deadlineMillis) {
        iter.remove();
      }
    }
  }

  @Override
  public boolean isEmpty() {
    return pendingQueries.isEmpty();
  }

  @Override
  public int numPending() {
    return pendingQueries.size();
  }

  int getAvailableTokens() {
    tokenLock.lock();
    try {
      consumeTokens();
      return availableTokens;
    } finally {
      tokenLock.unlock();
    }
  }

  @Override
  public int getThreadsInUse() {
    return threadsInUse;
  }

  @Override
  public void addReservedThreads(int threads) {
    reservedThreads.addAndGet(threads);
  }

  @Override
  public void releasedReservedThreads(int threads) {
    reservedThreads.addAndGet(-1 * threads);
  }

  @Override
  public int totalReservedThreads() {
    return reservedThreads.get();
  }

  @Override
  public void incrementThreads() {
    tokenLock.lock();
    try {
      incrementThreadsInternal();
    } finally {
      tokenLock.unlock();
    }
  }

  @Override
  public void decrementThreads() {
    tokenLock.lock();
    try {
      consumeTokens();
      --threadsInUse;
    } finally {
      tokenLock.unlock();
    }
  }

  @Override
  public void startQuery() {
    tokenLock.lock();
    try {
      incrementThreadsInternal();
      ++runningQueries;
    } finally {
      tokenLock.unlock();
    }
  }

  @Override
  public void endQuery() {
    tokenLock.lock();
    try {
      decrementThreadsInternal();
      --runningQueries;
    } finally {
      tokenLock.unlock();
    }
  }

  // callers must synchronize access to this method
  private void consumeTokens() {
    long currentTimeMs = System.currentTimeMillis();
    // multiple time qantas may have elapsed..hence, the modulo operation
    int diffMs = (int) (currentTimeMs - lastUpdateTimeMs);
    if (diffMs <= 0) {
      return;
    }
    long nextTokenTime = lastTokenTimeMs + tokenLifetimeMs;
    if (nextTokenTime > currentTimeMs) {
      availableTokens -= diffMs * threadsInUse;
    } else {
      availableTokens -= (nextTokenTime - lastUpdateTimeMs) * threadsInUse;
      // for each quantum allocate new set of tokens with linear decay of tokens.
      // Linear decay lowers the tokens available to heavy users in the next period
      // allowing light users to have better chance at scheduling. Without linear decay,
      // groups with high request rate will win more often putting light users at disadvantage.
      for (; nextTokenTime <= currentTimeMs; nextTokenTime += tokenLifetimeMs) {
        availableTokens  = (int) (ALPHA * tokenLifetimeMs * numTokensPerMs +
            (1-ALPHA) * (availableTokens - tokenLifetimeMs * threadsInUse));
      }
      lastTokenTimeMs = nextTokenTime - tokenLifetimeMs;
      availableTokens -= (currentTimeMs - lastTokenTimeMs) * threadsInUse;
    }
    lastUpdateTimeMs = currentTimeMs;
  }

  private void incrementThreadsInternal() {
    consumeTokens();
    ++threadsInUse;
  }

  private void decrementThreadsInternal() {
    consumeTokens();
    --threadsInUse;
  }

  /**
   * Compares priority of this group with respect to another scheduler group.
   * Priority is compared on the basis of available tokens. SchedulerGroup with
   * higher number of tokens wins. If both groups have same tokens then the group
   * with earliest waiting job has higher priority (FCFS if tokens are equal).
   * If the arrival times of first waiting jobs are also equal then the group
   * with least reserved resources is selected
   * @param rhs SchedulerGroupAccount to compare with
   * @return < 0 if lhs has lower priority than rhs
   *     > 0 if lhs has higher priority than rhs
   *     = 0 if lhs has same priority as rhs
   */
  @Override
  public int compareTo(SchedulerGroupAccountant rhs) {
    if (rhs == null) {
      return 1;
    }
    int leftTokens = getAvailableTokens();
    int rightTokens = ((SchedulerTokenGroup) rhs).getAvailableTokens();
    if (leftTokens > rightTokens) {
      return 1;
    }
    if (leftTokens < rightTokens) {
      return -1;
    }

    return compareArrivalTimes(((SchedulerTokenGroup) rhs));
  }

  private int compareArrivalTimes(SchedulerTokenGroup rhs) {
    long leftArrivalMs = peekFirst().getArrivalTimeMs();
    long rightArrivalMs = rhs.peekFirst().getArrivalTimeMs();
    if (leftArrivalMs < rightArrivalMs) {
      return 1;
    }
    if (leftArrivalMs > rightArrivalMs) {
      return -1;
    }
    return 0;
  }
}
