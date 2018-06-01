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
    * Constants for typical user metadata keys.
    */
   public static class MetaKeys {
      /**
       * A nickname {@value}.
       */
      public static final String NICKNAME = "nickname";

      /**
       * A first name {@value}.
       */
      public static final String FIRST_NAME = "first_name";

      /**
       * A last name {@value}.
       */
      public static final String LAST_NAME = "last_name";

      /**
       * A description {@value}.
       */
      public static final String DESCRIPTION = "description";

      /**
       * The capabilities (serialized PHP) {@value}.
       */
      public static final String CAPABILITIES = "wp_capabilities";

      /**
       * The user level {@value}.
       */
      public static final String USER_LEVEL = "wp_user_level";

      /**
       * The source domain {@value}.
       */
      public static final String SOURCE_DOMAIN = "source_domain";

      /**
       * The primary blog {@value}.
       */
      public static final String PRIMARY_BLOG = "primary_blog";

      /**
       * The locale {@value}.
       */
      public static final String LOCALE = "locale";
   }

   /**
    * Creates a user with a slug.
    * @param id The unique id.
    * @param username The username.
    * @param displayName The display name.
    * @param slug The slug.
    * @param email The email.
    * @param createTimestamp The create timestamp.
    * @param url The URL.
    * @param metadata Associated metadata.
    */
   public User(final long id, final String username, final String displayName,
               final String slug,
               final String email,
               final long createTimestamp,
               final String url,
               final Collection<Meta> metadata) {
      this.id = id;
      this.username = username;
      this.displayName = displayName;
      this.slug = slug;
      this.email = email;
      this.createTimestamp = createTimestamp;
      this.url = url;
      this.metadata = metadata != null ? ImmutableList.copyOf(metadata) : ImmutableList.of();
   }


   /**
    * Creates a user with a generated slug.
    * @param id The unique id.
    * @param username The username.
    * @param displayName The display name.
    * @param email The email.
    * @param createTimestamp The create timestamp.
    * @param url The URL.
    * @param metadata Associated metadata.
    */
   public User(final long id, final String username, final String displayName,
               final String email,
               final long createTimestamp,
               final String url,
               final Collection<Meta> metadata) {
      this.id = id;
      this.username = username;
      this.displayName = displayName;
      this.email = email;
      this.createTimestamp = createTimestamp;
      this.url = url;
      this.metadata = metadata != null ? ImmutableList.copyOf(metadata) : ImmutableList.of();
      this.slug = slugify(displayName());
   }

   /**
    * Creates a user with a new id.
    * @param id The new id.
    * @return The user with new id.
    */
   public User withId(final long id) {
      return new User(id, username, displayName, slug, email, createTimestamp, url, metadata);
   }

   /**
    * Creates a user with added metadata.
    * @param metadata The metadata.
    * @return The user with metadata added.
    */
   public User withMetadata(final List<Meta> metadata) {
      return new User(id, username, displayName, slug, email, createTimestamp, url, metadata);
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
    * The URL.
    */
   public final String url;

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
