/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.retry;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.util.Clock;

import java.util.Iterator;

/**
 * <p>Sliding window retry policy for relaunching a
 * <code>Container</code> in Yarn</p>
 */
@InterfaceStability.Unstable
public class SlidingWindowRetryPolicy {

  private Clock clock;

  public SlidingWindowRetryPolicy(Clock clock)  {
    this.clock = Preconditions.checkNotNull(clock);
  }

  public boolean shouldRetry(ContainerRetryContext retryContext,
      int errorCode) {
    ContainerRetryPolicy retryPolicy = retryContext.getRetryPolicy();
    if (retryPolicy == ContainerRetryPolicy.RETRY_ON_ALL_ERRORS
        || (retryPolicy == ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES
        && retryContext.getErrorCodes() != null
        && retryContext.getErrorCodes().contains(errorCode))) {
      updateRetryContext(retryContext);
      return retryContext.getRemainingRetries() > 0 ||
          retryContext.getMaxRetries() == ContainerRetryContext.RETRY_FOREVER;
    }
    return false;
  }


  /**
   * Update restart time if failuresValidityInterval is > 0 and
   * maxRetries is not RETRY_FOREVER. Otherwise, when
   * failuresValidityInterval is < 0 or maxRetries is RETRY_FOREVER
   * we just record remaining retry attempts and avoid recording each restart
   * time.
   */
  private void updateRetryContext(ContainerRetryContext retryContext) {
    if (retryContext.getMaxRetries() !=
        ContainerRetryContext.RETRY_FOREVER &&
        retryContext.getFailuresValidityInterval() > 0) {

      Iterator<Long> iterator = retryContext.getRestartTimes().iterator();
      long currentTime = clock.getTime();
      while (iterator.hasNext()) {
        long restartTime = iterator.next();
        if (currentTime - restartTime >
            retryContext.getFailuresValidityInterval()) {
          iterator.remove();
        } else {
          break;
        }
      }
      int remainingRetries = retryContext.getMaxRetries() -
          retryContext.getRestartTimes().size();
      retryContext.setRemainingRetries(remainingRetries);
      retryContext.getRestartTimes().add(clock.getTime());
    } else {
      int remainingRetries = retryContext.getRemainingRetries() - 1;
      retryContext.setRemainingRetries(remainingRetries);
    }
  }

  public void setClock(Clock clock) {
    this.clock = Preconditions.checkNotNull(clock);
  }
}
