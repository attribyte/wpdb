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
 * A term as referenced in a taxonomy.
 */
public class TaxonomyTerm {

   /**
    * Creates a term in a taxonomy with no parent.
    * @param id The id assigned to the term in the taxonomy.
    * @param taxonomy The taxonomy.
    * @param term The term.
    * @param description The description as it appears in the taxonomy.
    */
   public TaxonomyTerm(final long id, final String taxonomy,
                       final Term term,
                       final String description) {
      this(id, taxonomy, term, description, 0L);
   }

   /**
    * Creates a term in a taxonomy with a parent.
    * @param id The id assigned to the term in the taxonomy.
    * @param taxonomy The taxonomy.
    * @param term The term.
    * @param description The description as it appears in the taxonomy.
    * @param parentId The parent id, if any.
    */
   public TaxonomyTerm(final long id, final String taxonomy,
                       final Term term,
                       final String description, final long parentId) {
      this.id = id;
      this.taxonomy = taxonomy;
      this.term = term;
      this.description = description;
      this.parentId = parentId;
   }

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id)
              .add("taxonomy", taxonomy)
              .add("term", term)
              .add("description", description)
              .add("parentId", parentId)
              .toString();
   }

   /**
    * The id assigned to the term in the taxonomy.
    */
   public final long id;

   /**
    * The taxonomy.
    */
   public final String taxonomy;

   /**
    * The associated term.
    */
   public final Term term;

   /**
    * The description of the term as it appears in the taxonomy.
    */
   public final String description;

   /**
    * The parent taxonomy id, if any.
    */
   public final long parentId;
}