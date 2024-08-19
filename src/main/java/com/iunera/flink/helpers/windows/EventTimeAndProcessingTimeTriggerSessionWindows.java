package com.iunera.flink.helpers.windows;

/*-
 * #%L
 * iu-flink-extensions
 * %%
 * Copyright (C) 2024 Tim Frey, Christian Schmitt
 * %%
 * Licensed under the OPEN COMPENSATION TOKEN LICENSE (the "License").
 *
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * <https://github.com/open-compensation-token-license/license/blob/main/LICENSE.md>
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @octl.sid: 1b6f7a5d-8dcf-44f1-b03a-77af04433496
 * #L%
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link WindowAssigner} that windows elements into sessions based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * <p>For example, in order to window into windows of 1 minute, every 10 seconds:
 *
 * <pre>{@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(EventTimeSessionWindows.withGap(Time.minutes(1)));
 * }</pre>
 */
public class EventTimeAndProcessingTimeTriggerSessionWindows
    extends MergingWindowAssigner<Object, TimeWindow> {
  private static final long serialVersionUID = 1L;

  protected long sessionTimeout;
  protected long latenessTimeout;

  protected EventTimeAndProcessingTimeTriggerSessionWindows(
      long sessionTimeout, long latenessTimeout) {
    if (sessionTimeout <= 0) {
      throw new IllegalArgumentException(
          "EventTimeSessionWindows parameters must satisfy 0 < size");
    }

    this.sessionTimeout = sessionTimeout;
    this.latenessTimeout = latenessTimeout;
  }

  @Override
  public Collection<TimeWindow> assignWindows(
      Object element, long timestamp, WindowAssignerContext context) {
    return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
  }

  @Override
  public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
    if (this.trigger != null) return trigger;
    return EventTimeAndProcessingTimeTrigger.create(this.latenessTimeout);
  }

  private Trigger<Object, TimeWindow> trigger;

  public EventTimeAndProcessingTimeTriggerSessionWindows trigger(
      Trigger<Object, TimeWindow> trigger) {
    this.trigger = trigger;
    return this;
  }

  @Override
  public String toString() {
    return "EventTimeSessionWindows(" + sessionTimeout + ")";
  }

  /**
   * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns elements to sessions
   * based on the element timestamp.
   *
   * @param size The session timeout, i.e. the time gap between sessions
   * @return The policy.
   */
  public static EventTimeAndProcessingTimeTriggerSessionWindows withGap(
      Time size, Time additionalLatenessTimeout) {
    return new EventTimeAndProcessingTimeTriggerSessionWindows(
        size.toMilliseconds(), additionalLatenessTimeout.toMilliseconds());
  }

  /**
   * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns elements to sessions
   * based on the element timestamp.
   *
   * @param sessionWindowTimeGapExtractor The extractor to use to extract the time gap from the
   *     input elements
   * @return The policy.
   */
  //    @PublicEvolving
  //    public static <T> DynamicEventTimeSessionWindows<T> withDynamicGap(
  //            SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor) {
  //        return new DynamicEventTimeSessionWindows<>(sessionWindowTimeGapExtractor);
  //    }

  @Override
  public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
    return new TimeWindow.Serializer();
  }

  @Override
  public boolean isEventTime() {
    return true;
  }

  /** Merge overlapping {@link TimeWindow}s. */
  @Override
  public void mergeWindows(
      Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {
    TimeWindow.mergeWindows(windows, c);
  }
}
