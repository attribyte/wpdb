/*
 * Copyright 2016 Attribyte, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.attribyte.wp.model;

/**
 * Metadata associated with a post, user, or
 * other object.
 */
public class Meta {

   /**
    * Creates metadata.
    * @param id The id.
    * @param key The key.
    * @param value The value.
    */
   public Meta(final long id, final String key, final String value) {
      this.id = id;
      this.key = key;
      this.value = value;
   }

   /**
    * The unique id.
    */
   public final long id;

   /**
    * The key.
    */
   public final String key;

   /**
    * The value.
    */
   public final String value;
}
