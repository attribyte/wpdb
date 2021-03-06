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

package org.attribyte.wp;

public class Util {

   /**
    * The taxonomy for categories.
    */
   public static final String CATEGORY_TAXONOMY = "category";

   /**
    * The taxonomy for tags.
    */
   public static final String TAG_TAXONOMY = "post_tag";

   /**
    * The key for featured images added as post metadata.
    * <p>
    *    Note: id maps to a post of type "attachment."
    * </p>
    */
   public static final String FEATURED_IMAGE_KEY = "_thumbnail_id";

   /**
    * Creates a slug from a string.
    * <p>
    *    Does not strip markup.
    * </p>
    * @param str The string.
    * @return The slug for the string.
    */
   public static final String slugify(final String str) {
      StringBuilder buf = new StringBuilder();
      boolean lastWasDash = false;
      for(char ch : str.toLowerCase().trim().toCharArray()) {
         if(Character.isLetterOrDigit(ch)) {
            buf.append(ch);
            lastWasDash = false;
         } else {
            if(!lastWasDash) {
               buf.append("-");
               lastWasDash = true;
            }
         }
      }

      String slug = buf.toString();
      if(slug.length() == 0) {
         return "";
      }

      if(slug.charAt(0) == '-') {
         slug = slug.substring(1);
      }
      if(slug.length() > 0 && slug.charAt(slug.length() - 1) == '-') {
         slug = slug.substring(0, slug.length() - 1);
      }

      return slug;
   }
}
