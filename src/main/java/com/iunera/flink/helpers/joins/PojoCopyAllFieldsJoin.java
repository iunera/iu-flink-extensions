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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Copies all fields from one source class to a target class based on the name. This makes it
 * possible to use the same field names in the target class than in the source without having a
 * necessity for inheritance. Optionally, certain fields can be ignored when they are specified in
 * the constructor. The copy function requires the same field names in source and target pojo
 */
public class PojoCopyAllFieldsJoin<T, F> implements JoinFunction<T, F, T> {

  private Set<String> ignorefileds = new HashSet<>();

  public PojoCopyAllFieldsJoin(String... ignorefileds) {
    this.ignorefileds.addAll(Arrays.asList(ignorefileds));
  }

  @Override
  public T join(T target, F source) {
    copyAllFields(target, source);
    return target;
  }

  private <T> void copyAllFields(T to, F from) {
    // when it is a left outer join the from can be null and nothing can be copied
    if (from == null) return;
    Class<T> clazz = (Class<T>) from.getClass();
    List<Field> fields = getAllModelFields(clazz);

    Class<T> toclazz = (Class<T>) to.getClass();

    if (fields != null) {
      for (Field field : fields) {
        if (ignorefileds.contains(field.getName())) continue;
        try {
          field.setAccessible(true);
          Field tofield = toclazz.getDeclaredField(field.getName());
          tofield.setAccessible(true);
          tofield.set(to, field.get(from));
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        } catch (NoSuchFieldException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (SecurityException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (NullPointerException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  private static List<Field> getAllModelFields(Class aClass) {
    List<Field> fields = new ArrayList<>();
    do {
      Collections.addAll(fields, aClass.getDeclaredFields());
      aClass = aClass.getSuperclass();
    } while (aClass != null);
    return fields;
  }
}
