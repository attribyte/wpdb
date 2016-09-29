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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A post that represents an image attachment.
 */
public class ImageAttachment extends Post {

   /**
    * Creates an image attachment with unassigned id.
    * @param parent The parent post.
    * @param path The image path.
    * @param slug The image slug.
    * @param mimeType The associated mime type.
    */
   public ImageAttachment(final Post parent, final String path, final String slug, final String mimeType) {
      this(parent, 0L, path, slug, mimeType);
   }

   /**
    * Creates an image attachment with assigned id.
    * @param parent The parent post.
    * @param id The id.
    * @param path The image path.
    * @param slug The image slug.
    * @param mimeType The associated mime type.
    */
   public ImageAttachment(final Post parent, final long id, final String path, final String slug, final String mimeType) {
      super(id, slug, slug, "", "", parent.authorId, parent.author, parent.publishTimestamp, parent.publishTimestamp,
              parent.status, parent.id, path, 0, ImmutableList.of(), Type.ATTACHMENT, mimeType,
              ImmutableMap.of(), ImmutableList.of());
   }
}