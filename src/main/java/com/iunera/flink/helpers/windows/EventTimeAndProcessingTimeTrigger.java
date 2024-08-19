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

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Trigger that works with processing time and event time. This trigger ensures that events that
 * have been provided in the wrong order still will be processed. Two cases:
 *
 * <p>1: Imagine a late event at processing time having an older event time than than a priorly
 * processed event. In this case, the trigger will wait forever. For this reason, this trigger
 * offers a fallback to processing time, that waits the window time plus an out of orderness value
 * to process.
 *
 * <p>2: When an event in a stream does not have a succeeding event and one processes by event time,
 * the firing of the window will wait till there is a newer event with a more recent timestamp. This
 * is also solved by waiting the event time for a new event.
 *
 * <p>Maximum lateness for events is the window size plus a manually defined lateness factor. The
 * maximum window size is required, because the latest event could be received first and the the
 * first event of the window last.
 */
public class EventTimeAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
  private static final long serialVersionUID = 1L;

  private EventTimeAndProcessingTimeTrigger() {}

  private EventTimeAndProcessingTimeTrigger(long latenessTimeout) {
    this.latenessTimeout = latenessTimeout;
  }

  private long latenessTimeout = 0;
  private ValueStateDescriptor<Long> idleTimeTrigger =
      new ValueStateDescriptor<>("idleTimeTrigger", Long.class);

  @Override
  public TriggerResult onElement(
      Object element, long timestamp, TimeWindow window, TriggerContext ctx) {

    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
      // if the watermark is already past the window fire immediately
      return TriggerResult.FIRE;
    }

    setOrRemoveInactivityTimeout(window, ctx, true);

    ctx.registerEventTimeTimer(window.maxTimestamp());
    return TriggerResult.CONTINUE;
  }

  private void setOrRemoveInactivityTimeout(TimeWindow window, TriggerContext ctx, boolean setnew) {
    ValueState<Long> latestgeoHashstate = ctx.getPartitionedState(idleTimeTrigger);

    Long lastidleTimeTrigger;
    try {
      lastidleTimeTrigger = latestgeoHashstate.value();

      if (lastidleTimeTrigger != null) {
        ctx.deleteProcessingTimeTimer(lastidleTimeTrigger);
        latestgeoHashstate.update(null);
      }

      if (setnew) {
        // we need the complete window size + a possible lateness buffer, because the latest event
        // could be received first and the the first event of the window last

        long newlastidleTimeTrigger =
            window.maxTimestamp()
                - window.getStart()
                + ctx.getCurrentProcessingTime()
                + this.latenessTimeout;

        if (lastidleTimeTrigger == null || lastidleTimeTrigger < newlastidleTimeTrigger) {
          lastidleTimeTrigger = newlastidleTimeTrigger;
          ctx.registerProcessingTimeTimer(lastidleTimeTrigger);
          latestgeoHashstate.update(lastidleTimeTrigger);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    System.out.println("onEventTime");
    // return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    if (ctx.getCurrentProcessingTime() >= window.maxTimestamp() + this.latenessTimeout
        || time >= window.maxTimestamp()) {
      setOrRemoveInactivityTimeout(window, ctx, false);

      return TriggerResult.FIRE;
    } else {

      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
    if (time >= window.maxTimestamp()) {
      System.out.println("onProcessingTime");
      setOrRemoveInactivityTimeout(window, ctx, false);

      // TODO: I think we need here to check if the event time trigger was done
      // already

      // ctx.deleteEventTimeTimer(window.maxTimestamp());
      return TriggerResult.FIRE;
    }
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    setOrRemoveInactivityTimeout(window, ctx, false);

    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public boolean canMerge() {
    return true;
  }

  @Override
  public void onMerge(TimeWindow window, OnMergeContext ctx) {
    // only register a timer if the time is not yet past the end of the merged
    // window
    // this is in line with the logic in onElement(). If the time is past the end of
    // the window onElement() will fire and setting a timer here would fire the
    // window twice.
    long windowMaxTimestamp = window.maxTimestamp();

    ValueState<Long> latestgeoHashstate = ctx.getPartitionedState(idleTimeTrigger);

    setOrRemoveInactivityTimeout(window, ctx, false);

    if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
      ctx.registerEventTimeTimer(windowMaxTimestamp);
    }
  }

  @Override
  public String toString() {
    return "EventTimeTriggerWithIdleDetection()";
  }

  /** Creates a new trigger that fires once system time passes the end of the window. */
  public static EventTimeAndProcessingTimeTrigger create(long maxOutOfOrderness) {
    return new EventTimeAndProcessingTimeTrigger(maxOutOfOrderness);
  }
}
