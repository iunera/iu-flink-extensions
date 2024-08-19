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
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;

/** Allows to serialize a POJO to a CSV with the headline of the column names as the column name. */
public class PojoCSVWriter<T> implements TextFormatter<T> {

  private boolean isfirstline = true;

  @Override
  public String format(T obj) {
    StringBuilder csvRow = new StringBuilder();

    Field fields[] = obj.getClass().getDeclaredFields();

    if (isfirstline) {
      appendHeader(csvRow, fields);
      isfirstline = false;
    }

    boolean firstField = true;
    for (Field field : fields) {
      field.setAccessible(true);
      Object value;
      try {
        value = field.get(obj);
        if (value == null) value = "";
        if (firstField) {

          firstField = false;
        } else {
          csvRow.append(";");
        }

        if (field.getType().isArray()) {
          csvRow.append(convertObjectArrayToString((Object[]) value));
        } else {
          csvRow.append(value);
        }

        field.setAccessible(false);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        // LOGGER.severe(e.toString());
        e.printStackTrace();
      }
    }
    return csvRow.toString();
  }

  private void appendHeader(StringBuilder csvRow, Field fields[]) {
    boolean firstField = true;
    for (Field field : fields) {
      if (firstField) {

        firstField = false;
      } else {
        csvRow.append(";");
      }
      csvRow.append(field.getName());
    }
    csvRow.append(System.getProperty("line.separator"));
  }

  private static StringBuilder convertObjectArrayToString(Object[] arr) {

    StringBuilder sb = new StringBuilder("[");
    boolean firstField = true;
    for (Object obj : arr) {
      if (firstField) {

        firstField = false;
      } else {
        sb.append(",");
      }
      sb.append(obj.toString());
    }
    return sb.append("]");
  }
}
