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
 * An immutable term.
 */
public class Term {

   /**
    * Creates a term.
    * @param id The term id.
    * @param name The name.
    * @param slug The slug.
    */
   public Term(final long id, final String name, final String slug) {
      this.id = id;
      this.name = name;
      this.slug = slug;
   }

   /**
    * The id.
    */
   public final long id;

   /**
    * The name.
    */
   public final String name;

   /**
    * The slug.
    */
   public final String slug;
}
