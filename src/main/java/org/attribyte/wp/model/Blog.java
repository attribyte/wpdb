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

import com.google.common.base.MoreObjects;

/**
 * An immutable blog.
 */
public class Blog {

   /**
    * Creates a blog.
    * @param id The blog id.
    * @param siteId The associated site id.
    * @param domain The domain.
    * @param path The path.
    * @param registeredTimestamp The time the blog was registered.
    * @param updatedTimestamp The time the blog was last updated.
    */
   public Blog(final long id, final long siteId, final String domain,
               final String path, final long registeredTimestamp, final long updatedTimestamp) {
      this.id = id;
      this.siteId = siteId;
      this.domain = domain;
      this.path = path;
      this.registeredTimestamp = registeredTimestamp;
      this.updatedTimestamp = updatedTimestamp;
   }

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id)
              .add("siteId", siteId)
              .add("domain", domain)
              .add("path", path)
              .add("registeredTimestamp", registeredTimestamp)
              .add("updatedTimestamp", updatedTimestamp)
              .toString();
   }

   /**
    * The blog id.
    */
   public final long id;

   /**
    * The associated site id.
    */
   public final long siteId;

   /**
    * The domain.
    */
   public final String domain;

   /**
    * The blog path.
    */
   public final String path;

   /**
    * The time the blog was registered.
    */
   public final long registeredTimestamp;

   /**
    * The time the blog was last updated.
    */
   public final long updatedTimestamp;

}
