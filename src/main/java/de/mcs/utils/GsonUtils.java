/**
 * Copyright 2019 w.klaas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package de.mcs.utils;

/*-
 * #%L
 * Client lib for connecting to EASY Cloud Archive
 * %%
 * Copyright (C) 2019 EASY SOFTWARE AG
 * %%
 * EASY Cloud Archive
 * Copyright (c) 2019 by EASY ENTERPRISE SERVICES GmbH
 * License: https://nx.easy.de/nexus/content/sites/espub/license.txt
 * --------------------------------------
 * #L%
 */

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.google.gson.Gson;

/**
 * @author w.klaas
 *
 */
public class GsonUtils {

  private static Yaml yaml;

  public static Yaml getYamlMapper() {
    if (yaml == null) {
      yaml = new Yaml();
    }
    return yaml;
  }

  public static Yaml getYamlMapper(Class clazz) {
    return new Yaml(new Constructor(clazz));
  }

  private static Gson gson;

  public static Gson getJsonMapper() {
    if (gson == null) {
      gson = new Gson();
      // jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      // jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
      // jsonObjectMapper.setSerializationInclusion(Include.NON_NULL);
    }
    return gson;
  }

}
