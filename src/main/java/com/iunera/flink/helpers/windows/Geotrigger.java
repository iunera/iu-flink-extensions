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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.OnMergeContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * This allows stream processing triggers to be fired once a geolocation changes. Imagine you want
 * to start processing forecasts how many people will arrive at the next bus stop and if the bus is
 * empty of full. With this trigger one can get all door in and out events of passengers and once
 * the bus changes the geoposition one knows that it is departed and processing can start. Likewise
 * is for other traffic scenarios such a trigger can aggregate and reduce (big) data by firing
 * triggers for aggregation of the data once a geopsoition changed. This way, fine grained
 * geoposition data can be aggregated into a forced grained tile of geoposition data.
 *
 * <p>Use like: EventTimeAndProcessingTimeTriggerSessionWindows.withGap(Time.minutes(1),
 * Time.seconds(1)) .trigger( new Geotrigger(
 * EventTimeAndProcessingTimeTrigger.create(Time.seconds(1).toMilliseconds()), new
 * KeySelector<RideEvent, String>() {
 *
 * <p>public String getKey(RideEvent element) throws Exception { //Use for example a geohash or
 * Mercator projections. //In the following example, once the geohash changes he trigger is fired.
 * //Here we have the precision of 8 .. to make it more coarse grained take 7 return
 * ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision(element.latitude, element.longitude,
 * 8); } }) )
 */
public class Geotrigger<IN, KEY> extends Trigger<Object, TimeWindow> {

  private Trigger<Object, TimeWindow> originaltrigger;
  private KeySelector<IN, String> keyselector;

  private ValueStateDescriptor<String> geoTrigger =
      new ValueStateDescriptor<String>("geoTrigger", TypeInformation.of(new TypeHint<String>() {}));

  private Geotrigger() {}

  public Geotrigger(
      Trigger<Object, TimeWindow> wrappedTrigger, KeySelector<IN, String> keyselector) {
    this.originaltrigger = wrappedTrigger;
    this.keyselector = keyselector;
  }

  @Override
  public TriggerResult onElement(
      Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

    ValueState<String> latestgeoHashstate = ctx.getPartitionedState(geoTrigger);

    String lastidleTimeTrigger;
    try {
      lastidleTimeTrigger = latestgeoHashstate.value();

      String current = keyselector.getKey((IN) element);

      if (lastidleTimeTrigger == null) {
        lastidleTimeTrigger = current;
        latestgeoHashstate.update(current);
      }

      if (lastidleTimeTrigger.equals(current)) {

      } else {
        // the key value changed
        latestgeoHashstate.update(current);
        return TriggerResult.FIRE;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return this.originaltrigger.onElement(element, timestamp, window, ctx);
  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    return this.originaltrigger.onProcessingTime(time, window, ctx);
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    return this.originaltrigger.onEventTime(time, window, ctx);
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ValueState<String> latestgeoHashstate = ctx.getPartitionedState(geoTrigger);
    latestgeoHashstate.update(null);
    this.originaltrigger.clear(window, ctx);
  }

  @Override
  public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
    this.originaltrigger.onMerge(window, ctx);
  }
}
