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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.attribyte.wp.Util.TAG_TAXONOMY;
import static org.attribyte.wp.Util.CATEGORY_TAXONOMY;

/**
 * An immutable post.
 */
public class Post {

   /**
    * Post status.
    */
   public enum Status {

      /**
       * Post is a draft.
       */
      DRAFT,

      /**
       * Post is auto-draft.
       */
      AUTO_DRAFT,

      /**
       * Post is pending publication.
       */
      PENDING,

      /**
       * Post is private.
       */
      PRIVATE,

      /**
       * Post is published.
       */
      PUBLISH,

      /**
       * Post is scheduled to be published.
       */
      FUTURE,

      /**
       * Status is inherited from the parent.
       */
      INHERIT,

      /**
       * Post status is unknown.
       */
      UNKNOWN;

      /**
       * Creates status from a string.
       * @param str The status string.
       * @return The status.
       */
      public static Status fromString(final String str) {
         switch(Strings.nullToEmpty(str).trim().toLowerCase()) {
            case "draft" : return DRAFT;
            case "auto-draft" : return AUTO_DRAFT;
            case "pending" : return PENDING;
            case "private" : return PRIVATE;
            case "publish" : return PUBLISH;
            case "inherit" : return INHERIT;
            case "future" : return FUTURE;
            default: return UNKNOWN;
         }
      }
   }

   /**
    * The post type.
    */
   public enum Type {

      /**
       * Post is a regular post.
       */
      POST,

      /**
       * Post is a page.
       */
      PAGE,

      /**
       * Post is an attachment.
       */
      ATTACHMENT,

      /**
       * Post is a revision.
       */
      REVISION,

      /**
       * Post type is unknown.
       */
      UNKNOWN;

      /**
       * Creates a post type from a string.
       * @param str The type string.
       * @return The post type.
       */
      public static Type fromString(final String str) {
         switch(Strings.nullToEmpty(str).trim().toLowerCase()) {
            case "post" : return POST;
            case "page" : return PAGE;
            case "attachment" : return ATTACHMENT;
            case "revision" : return REVISION;
            default: return UNKNOWN;
         }
      }
   }

   public enum CommentStatus {

      /**
       * Comments are open.
       */
      OPEN,

      /**
       * Comments are close.
       */
      CLOSED,

      /**
       * Comment status is unknown.
       */
      UNKNOWN;

      /**
       * Creates comment status from a string.
       * @param str The string.
       * @return The comment status.
       */
      public static CommentStatus fromString(final String str) {
         switch(Strings.nullToEmpty(str).trim().toLowerCase()) {
            case "open" : return OPEN;
            case "closed" : return CLOSED;
            default: return UNKNOWN;
         }
      }
   }

   /**
    * Post sort methods.
    */
   public enum Sort {

      /**
       * Sort in ascending order by publish time.
       */
      ASC,

      /**
       * Sort in descending order by publish time.
       */
      DESC,

      /**
       * Sort in ascending order by last modified time.
       */
      ASC_MOD,

      /**
       * Sort in descending order by last modified time.
       */
      DESC_MOD,

      /**
       * Sort in ascending order of id.
       */
      ID_ASC,

      /**
       * Sort in descending order of id.
       */
      ID_DESC
   }

   /**
    * Builds an immutable post.
    */
   public static class Builder {

      /**
       * Gets the id.
       * @return The id.
       */
      public long getId() {
         return id;
      }

      /**
       * Sets the id.
       * @param id The id.
       * @return A self-reference.
       */
      public Builder setId(final long id) {
         this.id = id;
         return this;
      }

      /**
       * Gets the slug.
       * @return The slug.
       */
      public String getSlug() {
         return slug;
      }

      /**
       * Sets the slug.
       * @param slug The slug.
       * @return A self-reference.
       */
      public Builder setSlug(final String slug) {
         this.slug = slug;
         return this;
      }

      /**
       * Gets the title.
       * @return The title.
       */
      public String getTitle() {
         return title;
      }

      /**
       * Sets the title.
       * @param title The title.
       * @return A self-reference.
       */
      public Builder setTitle(final String title) {
         this.title = title;
         return this;
      }

      /**
       * Ges the excerpt.
       * @return The excerpt.
       */
      public String getExcerpt() {
         return excerpt;
      }

      /**
       * Sets the excerpt.
       * @param excerpt The excerpt.
       * @return A self-reference.
       */
      public Builder setExcerpt(final String excerpt) {
         this.excerpt = excerpt;
         return this;
      }

      /**
       * Gets the content.
       * @return The content.
       */
      public String getContent() {
         return content;
      }

      /**
       * Sets the content.
       * @param content The content.
       * @return A self-reference.
       */
      public Builder setContent(final String content) {
         this.content = content;
         return this;
      }

      /**
       * Gets the author id.
       * @return The author id.
       */
      public long getAuthorId() {
         return authorId;
      }

      /**
       * Sets the author id.
       * @param authorId The author id.
       * @return A self-reference.
       */
      public Builder setAuthorId(final long authorId) {
         this.authorId = authorId;
         return this;
      }

      /**
       * Gets the author.
       * @return The author, or {@code null} if not set.
       */
      public User getAuthor() {
         return author;
      }

      /**
       * Sets the author.
       * @param author The author.
       * @return A self-reference.
       */
      public Builder setAuthor(final User author) {
         this.author = author;
         return this;
      }

      /**
       * Gets the publish timestamp.
       * @return The publish timestamp.
       */
      public long getPublishTimestamp() {
         return publishTimestamp;
      }

      /**
       * Sets the publish timestamp.
       * @param publishTimestamp The publish timestamp.
       * @return A self reference.
       */
      public Builder setPublishTimestamp(final long publishTimestamp) {
         this.publishTimestamp = publishTimestamp;
         return this;
      }

      /**
       * Gets the last modified timestamp.
       * @return The last modified timestamp.
       */
      public long getModifiedTimestamp() {
         return modifiedTimestamp;
      }

      /**
       * Sets the last modified timestamp.
       * @param modifiedTimestamp The timestamp.
       * @return A self-reference.
       */
      public Builder setModifiedTimestamp(final long modifiedTimestamp) {
         this.modifiedTimestamp = modifiedTimestamp;
         return this;
      }

      /**
       * Gets the status.
       * @return The status.
       */
      public Status getStatus() {
         return status;
      }

      /**
       * Sets the status.
       * @param status The status.
       * @return A self-reference.
       */
      public Builder setStatus(final Status status) {
         this.status = status;
         return this;
      }

      /**
       * Gets the parent id.
       * @return The parent id.
       */
      public long getParentId() {
         return parentId;
      }

      /**
       * Sets the parent id.
       * @param parentId The parent id.
       * @return A self-reference.
       */
      public Builder setParentId(final long parentId) {
         this.parentId = parentId;
         return this;
      }

      /**
       * Gets the GUID.
       * @return The GUID.
       */
      public String getGUID() {
         return guid;
      }

      /**
       * Sets the GUID.
       * @param guid The GUID.
       * @return A self-reference.
       */
      public Builder setGUID(final String guid) {
         this.guid = guid;
         return this;
      }

      /**
       * Gets the comment count.
       * @return The comment count.
       */
      public int getCommentCount() {
         return commentCount;
      }

      /**
       * Sets the comment count.
       * @param commentCount The comment count.
       * @return A self-reference.
       */
      public Builder setCommentCount(final int commentCount) {
         this.commentCount = commentCount;
         return this;
      }

      /**
       * Gets the metadata.
       * @return The list of metadata.
       */
      public List<Meta> getMetadata() {
         return metadata;
      }

      /**
       * Sets the metadata.
       * @param metadata The metadata.
       * @return A self-reference.
       */
      public Builder setMetadata(final List<Meta> metadata) {
         this.metadata = metadata;
         return this;
      }

      /**
       * Gets the post type.
       * @return The type.
       */
      public Type getType() {
         return type;
      }

      /**
       * Sets the post type.
       * @param type The type.
       * @return A self-reference.
       */
      public Builder setType(final Type type) {
         this.type = type;
         return this;
      }

      /**
       * Sets terms for a taxonomy, replacing any existing.
       * @param terms The terms.
       * @return A self-reference.
       */
      public Builder setTaxonomyTerms(final List<TaxonomyTerm> terms) {
         if(taxonomyTerms == null) {
            taxonomyTerms = Maps.newHashMapWithExpectedSize(4);
         } else {
            taxonomyTerms.clear();
         }

         if(terms != null) {
            for(TaxonomyTerm term : terms) {
               addTaxonomyTerm(term);
            }
         }
         return this;
      }

      /**
       * Adds a taxonomy term.
       * @param term The term.
       * @return A self-reference.
       */
      public Builder addTaxonomyTerm(final TaxonomyTerm term) {
         if(taxonomyTerms == null) {
            taxonomyTerms = Maps.newLinkedHashMapWithExpectedSize(4);
         }
         List<TaxonomyTerm> terms = taxonomyTerms.get(term.taxonomy);
         if(terms == null) {
            terms = Lists.newArrayListWithExpectedSize(8);
            taxonomyTerms.put(term.taxonomy, terms);
         }
         terms.add(term);
         return this;
      }

      /**
       * Gets the mime type.
       * @return The mime type.
       */
      public String getMimeType() {
         return mimeType;
      }

      /**
       * Sets the mime type.
       * @param mimeType The mime type.
       * @return A self-reference;
       */
      public Builder setMimeType(final String mimeType) {
         this.mimeType = mimeType;
         return this;
      }

      /**
       * Gets the children.
       * @return The children.
       */
      public List<Post> getChildren() {
         return children;
      }

      /**
       * Sets the children.
       * @param children The children.
       */
      public void setChildren(final List<Post> children) {
         this.children = children;
      }

      /**
       * Creates an empty builder.
       */
      Builder() {
      }

      /**
       * Creates a builder with data from a post.
       * @param post The post.
       */
      Builder(final Post post) {
         this.id = post.id;
         this.slug = post.slug;
         this.title = post.title;
         this.excerpt = post.excerpt;
         this.content = post.content;
         this.authorId = post.authorId;
         this.author = post.author;
         this.publishTimestamp = post.publishTimestamp;
         this.modifiedTimestamp = post.modifiedTimestamp;
         this.status = post.status;
         this.parentId = post.parentId;
         this.guid = post.guid;
         this.commentCount = post.commentCount;
         this.metadata = post.metadata != null ? Lists.newArrayList(post.metadata) : Lists.newArrayList();
         this.type = post.type;
         this.mimeType = post.mimeType;
         this.taxonomyTerms = post.taxonomyTerms != null ? Maps.newHashMap(post.taxonomyTerms) : null;
         this.children = post.children != null ? Lists.newArrayList(post.children) : null;
      }

      /**
       * Builds an immutable post.
       * @return The post.
       */
      public Post build() {
         ImmutableMap.Builder<String, ImmutableList<TaxonomyTerm>> builder = ImmutableMap.builder();
         if(taxonomyTerms != null && taxonomyTerms.size() > 0) {
            taxonomyTerms.entrySet().forEach(kv -> builder.put(kv.getKey(), ImmutableList.copyOf(kv.getValue())));
         }
         return new Post(id, slug, title, excerpt, content, authorId, author,
                 publishTimestamp, modifiedTimestamp, status, parentId,
                 guid, commentCount, metadata, type, mimeType, builder.build(),
                 children != null ? ImmutableList.copyOf(children) : ImmutableList.of());
      }

      private long id;
      private String slug; //post_name
      private String title;
      private String excerpt;
      private String content;
      private long authorId;
      private User author;
      private long publishTimestamp;
      private long modifiedTimestamp;
      private Status status = Status.PUBLISH;
      private long parentId;
      private String guid;
      private int commentCount;
      private List<Meta> metadata;
      private Type type = Type.POST;
      private String mimeType;
      private Map<String, List<TaxonomyTerm>> taxonomyTerms;
      private List<Post> children;
   }

   /**
    * Creates an immutable post builder.
    * @return The new (empty) builder.
    */
   public static Builder newBuilder() {
      return new Builder();
   }

   /**
    * Creates an immutable post builder with data from an existing post.
    * @param post The post.
    * @return The new builder.
    */
   public static Builder newBuilder(final Post post) {
      return new Builder(post);
   }

   /**
    * Creates a copy of a post.
    * @param other The other post.
    */
   protected Post(final Post other) {
      this(other.id, other.slug, other.title, other.excerpt, other.content,
              other.authorId, other.author, other.publishTimestamp, other.modifiedTimestamp,
              other.status, other.parentId, other.guid, other.commentCount,
              other.metadata, other.type, other.mimeType, other.taxonomyTerms, other.children);
   }

   Post(final long id, final String slug, final String title, final String excerpt, final String content,
        final long authorId, final User author, final long publishTimestamp, final long modifiedTimestamp,
        final Status status, final long parentId, final String guid, final int commentCount,
        final Collection<Meta> metadata, final Type type,
        final String mimeType,
        final ImmutableMap<String, ImmutableList<TaxonomyTerm>> taxonomyTerms,
        final ImmutableList<Post> children) {
      this.id = id;
      this.slug = slug;
      this.title = title;
      this.excerpt = excerpt;
      this.content = content;
      this.authorId = authorId;
      this.author = author;
      this.publishTimestamp = publishTimestamp;
      this.modifiedTimestamp = modifiedTimestamp;
      this.status = status;
      this.parentId = parentId;
      this.guid = guid;
      this.commentCount = commentCount;
      this.metadata = metadata != null ? ImmutableList.copyOf(metadata) : ImmutableList.of();
      this.type = type;
      this.mimeType = mimeType;
      this.taxonomyTerms = taxonomyTerms;
      this.children = children;
   }

   /**
    * Changes the modified time of a post to the current time.
    * @return The post with modified time set to the current time.
    */
   public final Post modifiedNow() {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, System.currentTimeMillis(), status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Replaces post content.
    * @param content The new content.
    * @return The post with content replaced.
    */
   public final Post withContent(final String content) {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Adds an id to a post.
    * @param id The id.
    * @return The post with id added.
    */
   public final Post withId(final long id) {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Adds a parent id to a post.
    * @param parentId The parent id.
    * @return The post with parent added.
    */
   public final Post withParent(final long parentId) {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Adds an author to a post.
    * @param user The user that represents the author.
    * @return The post with author added.
    */
   public final Post withAuthor(final User user) {
      return new Post(id, slug, title, excerpt, content, authorId, user,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Adds taxonomy terms to a post.
    * @param taxonomyTerms The taxonomy terms.
    * @return The post with taxonomy terms added.
    */
   public final Post withTaxonomyTerms(final Map<String, List<TaxonomyTerm>> taxonomyTerms) {
      ImmutableMap.Builder<String, ImmutableList<TaxonomyTerm>> builder = ImmutableMap.builder();
      if(taxonomyTerms != null && taxonomyTerms.size() > 0) {
         taxonomyTerms.entrySet().forEach(kv -> builder.put(kv.getKey(), ImmutableList.copyOf(kv.getValue())));
      }
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, builder.build(), children);
   }

   /**
    * Adds metadata to a post.
    * @param metadata The metadata.
    * @return The post with metadata added.
    */
   public final Post withMetadata(final List<Meta> metadata) {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, metadata, type, mimeType, taxonomyTerms, children);
   }

   /**
    * Adds children to a post.
    * @param children The children to add.
    * @return The post with children added.
    */
   public final Post withChildren(final List<Post> children) {
      return new Post(id, slug, title, excerpt, content, authorId, author,
              publishTimestamp, modifiedTimestamp, status, parentId,
              guid, commentCount, this.metadata, type, mimeType, taxonomyTerms,
              children != null ? ImmutableList.copyOf(children) : ImmutableList.of());
   }

   /**
    * The unique id.
    */
   public final long id;

   /**
    * The slug.
    */
   public final String slug; //post_name

   /**
    * The title.
    */
   public final String title;

   /**
    * The excerpt.
    */
   public final String excerpt;

   /**
    * The content.
    */
   public final String content;

   /**
    * The author id.
    */
   public final long authorId;

   /**
    * The author.
    */
   public final User author;

   /**
    * The publish timestamp.
    */
   public final long publishTimestamp;

   /**
    * The modified timestamp.
    */
   public final long modifiedTimestamp;

   /**
    * The status.
    */
   public final Status status;

   /**
    * The parent id.
    */
   public final long parentId;

   /**
    * The GUID.
    */
   public final String guid;

   /**
    * The comment count.
    */
   public final int commentCount;

   /**
    * Metadata associated with the user.
    */
   public final ImmutableList<Meta> metadata;

   /**
    * The post type.
    */
   public final Type type;

   /**
    * The mime type.
    */
   public final String mimeType;

   /**
    * A map of taxonomy terms vs taxonomy name.
    */
   public final ImmutableMap<String, ImmutableList<TaxonomyTerm>> taxonomyTerms;

   /**
    * A list of children.
    */
   public final ImmutableList<Post> children;

   /**
    * An immutable list of terms associated with this post from a specified taxonomy.
    * @param taxonomy The taxonomy.
    * @return The list of terms.
    */
   public final ImmutableList<TaxonomyTerm> terms(final String taxonomy) {
      ImmutableList<TaxonomyTerm> terms = taxonomyTerms.get(taxonomy);
      return terms != null ? terms : ImmutableList.of();
   }


   /**
    * An immutable list of tags associated with this post.
    * @return The list of tags.
    */
   public final ImmutableList<TaxonomyTerm> tags() {
      ImmutableList<TaxonomyTerm> tags = taxonomyTerms.get(TAG_TAXONOMY);
      return tags != null ? tags : ImmutableList.of();
   }

   /**
    * An immutable list of categories associated with this post.
    * @return The list of categories.
    */
   public final ImmutableList<TaxonomyTerm> categories() {
      ImmutableList<TaxonomyTerm> categories = taxonomyTerms.get(CATEGORY_TAXONOMY);
      return categories != null ? categories : ImmutableList.of();
   }
}
