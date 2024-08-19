package com.iunera.flink.helpers.joins;

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

import java.lang.reflect.Field;
import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Copies a single field with a specified name from one Pojo T to another Pojo F to a field with a
 * specified name. Same like Copy all fields, but with the capability to have a mapping between the
 * names of fields. This enables data model and Pojo updates or to transform between between
 * different data models with different names.
 */
public class PojoFieldCopyJoin<T, F> implements JoinFunction<T, F, T> {
  private String from;
  private String to;

  public PojoFieldCopyJoin() {}

  /**
   * @param to the target field name
   * @param from the source field name
   */
  public PojoFieldCopyJoin(String to, String from) {
    this.from = from;
    this.to = to;
  }

  @Override
  public T join(T target, F origin) {

    try {
      Field targetfield = target.getClass().getField(to);
      targetfield.setAccessible(true);

      Field fromfield = origin.getClass().getField(from);
      fromfield.setAccessible(true);

      targetfield.set(target, fromfield.get(origin));
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return target;
  }
}
