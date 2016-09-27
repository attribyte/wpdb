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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

import static org.attribyte.wp.Util.slugify;

/**
 * An immutable user.
 */
public class User {

   /**
    * Creates a user.
    * @param id The unique id.
    * @param username The username.
    * @param displayName The display name.
    * @param email The email.
    * @param createTimestamp The create timestamp.
    * @param metadata Associated metadata.
    */
   public User(final long id, final String username, final String displayName,
               final String email,
               final long createTimestamp,
               final Collection<Meta> metadata) {
      this.id = id;
      this.username = username;
      this.displayName = displayName;
      this.email = email;
      this.createTimestamp = createTimestamp;
      this.metadata = metadata != null ? ImmutableList.copyOf(metadata) : ImmutableList.of();
      this.slug = slugify(displayName);
   }

   /**
    * Creates a user with a new id.
    * @param id The new id.
    * @return The user with new id.
    */
   public User withId(final long id) {
      return new User(id, username, displayName, email, createTimestamp, metadata);
   }

   /**
    * Creates a user with added metadata.
    * @param meta The metadata.
    * @return The user with metadata added.
    */
   public User withMetadata(final List<Meta> meta) {
      return new User(id, username, displayName, email, createTimestamp, metadata);
   }

   /**
    * The user id.
    */
   public final long id;

   /**
    * The username.
    */
   public final String username;

   /**
    * The display name, if set, otherwise the username.
    * @return The display name to use.
    */
   public final String displayName() {
      return Strings.nullToEmpty(displayName).trim().isEmpty() ? username : displayName;
   }

   /**
    * The display name.
    */
   public final String displayName;

   /**
    * The email.
    */
   public final String email;

   /**
    * The time user was created.
    */
   public final long createTimestamp;

   /**
    * Metadata associated with the user.
    */
   public final ImmutableList<Meta> metadata;

   /**
    * A slug for the author.
    */
   public final String slug;
}
