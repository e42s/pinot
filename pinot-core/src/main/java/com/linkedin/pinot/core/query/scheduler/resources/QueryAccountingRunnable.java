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
package com.linkedin.pinot.core.query.scheduler.resources;

import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import java.util.concurrent.Semaphore;


/**
 * Class to wrap query runnable to add accounting information
 */
class QueryAccountingRunnable implements Runnable {
  private final Runnable runnable;
  private final Semaphore semaphore;
  private final SchedulerGroupAccountant accountant;

  QueryAccountingRunnable(Runnable r, Semaphore semaphore, SchedulerGroupAccountant accountant) {
    this.runnable = r;
    this.semaphore = semaphore;
    this.accountant = accountant;
  }

  @Override
  public void run() {
    try {
      if (accountant != null) {
        accountant.incrementThreads();
      }
      runnable.run();
    } finally {
      if (accountant != null) {
        accountant.decrementThreads();
      }
      semaphore.release();
    }
  }
}
