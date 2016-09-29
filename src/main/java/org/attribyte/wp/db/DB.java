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

package org.attribyte.wp.db;


import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.sql.ConnectionSupplier;
import org.attribyte.util.SQLUtil;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Paging;
import org.attribyte.wp.model.Post;
import org.attribyte.wp.model.Site;
import org.attribyte.wp.model.TaxonomyTerm;
import org.attribyte.wp.model.Term;
import org.attribyte.wp.model.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.attribyte.util.SQLUtil.closeQuietly;
import static org.attribyte.wp.Util.CATEGORY_TAXONOMY;
import static org.attribyte.wp.Util.slugify;

public class DB implements MetricSet {

   /**
    * Creates a database with a connection supplier.
    * @param connectionSupplier The connection supplier.
    * @param siteId The site id.
    * @param cachedTaxonomies Enable caches for these taxonomies.
    * @param taxonomyCacheTimeout The expiration for taxonomy caches. If {@code 0}, caching is disabled.
    * @param userCacheTimeout The expiration for user caches. If {@code 0}, caching is disabled.
    */
   public DB(final ConnectionSupplier connectionSupplier,
             final long siteId,
             final Set<String> cachedTaxonomies,
             final Duration taxonomyCacheTimeout,
             final Duration userCacheTimeout) {
      this.connectionSupplier = connectionSupplier;
      this.siteId = siteId;

      final String postMetaTableName;
      final String termTaxonomyTableName;
      final String termsTableName;
      final String optionsTableName;

      if(siteId < 2) {
         this.postsTableName = "wp_posts";
         postMetaTableName = "wp_postmeta";
         optionsTableName = "wp_options";
         termsTableName = "wp_terms";
         termRelationshipsTableName = "wp_term_relationships";
         termTaxonomyTableName = "wp_term_taxonomy";
      } else {
         this.postsTableName = "wp_" + siteId + "_posts";
         postMetaTableName = "wp_" + siteId + "_postmeta";
         optionsTableName = "wp_" + siteId + "_options";
         termsTableName = "wp_" + siteId + "_terms";
         termRelationshipsTableName = "wp_" + siteId + "_term_relationships";
         termTaxonomyTableName = "wp_" + siteId + "_term_taxonomy";
      }

      this.userCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(userCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
              .build();

      this.usernameCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(userCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
              .build();

      ImmutableMap.Builder<String, Cache<String, TaxonomyTerm>> taxonomyTermCachesBuilder = ImmutableMap.builder();
      for(String taxonomy : cachedTaxonomies) {
         taxonomyTermCachesBuilder.put(taxonomy,
                 CacheBuilder.newBuilder()
                 .concurrencyLevel(4)
                 .expireAfterWrite(taxonomyCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
                 .build()
                 );
      }

      this.taxonomyTermCaches = taxonomyTermCachesBuilder.build();

      this.taxonomyTermCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(taxonomyCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
              .build();

      this.deletePostIdSQL = "DELETE FROM " + postsTableName + " WHERE ID=?";

      this.insertTermSQL = "INSERT INTO " + termsTableName + "(name, slug) VALUES (?, ?)";

      this.selectTermIdSQL = "SELECT name, slug FROM " + termsTableName + " WHERE term_id=?";

      this.selectTermIdsSQL = "SELECT term_id FROM " + termsTableName + " WHERE name=?";

      this.selectTaxonomyTermSQL = "SELECT term_taxonomy_id," + termTaxonomyTableName + ".term_id, description " +
              "FROM " + termsTableName + "," + termTaxonomyTableName + " WHERE " + termsTableName + ".name=? " +
              "AND taxonomy=? AND " +termsTableName + ".term_id=" + termTaxonomyTableName + ".term_id";

      this.selectTaxonomyTermIdSQL = "SELECT taxonomy, term_id, description FROM " + termTaxonomyTableName + " WHERE term_taxonomy_id=?";

      this.insertTaxonomyTermSQL = "INSERT INTO " + termTaxonomyTableName + "(term_id, taxonomy, description) VALUES (?,?, ?)";

      this.clearPostTermsSQL = "DELETE FROM " + termRelationshipsTableName + " WHERE object_id=?";

      this.clearPostTermSQL = "DELETE FROM " + termRelationshipsTableName + " WHERE object_id=? AND term_taxonomy_id=?";

      this.insertPostTermSQL = "INSERT IGNORE INTO " + termRelationshipsTableName + " (object_id, term_taxonomy_id, term_order) VALUES (?,?,?)";

      this.selectPostTermsSQL = "SELECT term_taxonomy_id FROM " + termRelationshipsTableName + " WHERE object_id=? ORDER BY term_order ASC";

      this.selectPostMetaSQL = "SELECT meta_id, meta_key, meta_value FROM " + postMetaTableName + " WHERE post_id=?";

      this.insertPostMetaSQL = "INSERT INTO " + postMetaTableName + "(post_id, meta_key, meta_value) VALUES (?,?,?)";

      this.deletePostMetaSQL = "DELETE FROM " + postMetaTableName + " WHERE post_id=?";

      this.selectOptionSQL = "SELECT option_value FROM " + optionsTableName + " WHERE option_name=?";

      this.selectPostsBySlugSQL = selectPostSQL + this.postsTableName + " WHERE post_name=? ORDER BY ID DESC";

      this.selectChildrenSQL = selectPostSQL + this.postsTableName + " WHERE post_parent=? ORDER BY ID DESC";

      this.insertPostWithIdSQL = "INSERT INTO " + postsTableName +
              " (ID, post_author, post_date, post_date_gmt, post_content, post_title, " +
                      "post_excerpt, post_status, post_name, post_modified, post_modified_gmt," +
                      "post_parent, guid, post_type, to_ping, pinged, post_content_filtered) VALUES " +
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?,?, '','', '')";

      this.insertPostSQL = "INSERT INTO " + postsTableName +
              " (post_author, post_date, post_date_gmt, post_content, post_title, " +
                      "post_excerpt, post_status, post_name, post_modified, post_modified_gmt," +
                      "post_parent, guid, post_type, to_ping, pinged, post_content_filtered) VALUES " +
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?, '','', '')";
   }

   private static final String createUserSQL =
           "INSERT INTO wp_users (user_login, user_pass, user_nicename, display_name, user_email, user_registered) " +
                   "VALUES (?, ?, ?, ?, ?, NOW())";

   /**
    * Creates a user.
    * @param user The user.
    * @param userPass The {@code user_pass} string to use (probably the hash of a default username/password).
    * @return The newly created user.
    * @throws SQLException on database error.
    */
   public User createUser(final User user, final String userPass) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      String nicename = user.displayName();
      if(nicename.length() > 49) {
         nicename = nicename.substring(0, 49);
      }

      String username = user.username.length() < 60 ? user.username : user.username.substring(0, 60);

      Timer.Context ctx = createUserTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(createUserSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setString(1, username);
         stmt.setString(2, Strings.nullToEmpty(userPass));
         stmt.setString(3, nicename);
         stmt.setString(4, user.displayName());
         stmt.setString(5, Strings.nullToEmpty(user.email));
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return user.withId(rs.getLong(1));
         } else {
            throw new SQLException("Problem creating user (no generated id)");
         }
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt, rs);
      }
   }

   private static final String selectUserSQL = "SELECT ID, user_login, user_nicename, display_name, user_email, user_registered FROM wp_users";

   /**
    * Creates a user from a result set.
    * @param rs The result set.
    * @return The user.
    * @throws SQLException on database error.
    */
   private User userFromResultSet(final ResultSet rs) throws SQLException {
      String niceName = Strings.nullToEmpty(rs.getString(3)).trim();
      String displayName = Strings.nullToEmpty(rs.getString(4)).trim();
      String useDisplayName = displayName.isEmpty() ? niceName : displayName;
      return new User(rs.getLong(1), rs.getString(2), useDisplayName, rs.getString(5), rs.getTimestamp(6).getTime(), ImmutableList.of());
   }

   private static final String selectUserByUsernameSQL = selectUserSQL + " WHERE user_login=?";

   /**
    * Selects a user by username.
    * <p>
    *    The user table is keyed to allow multiple users with the same username.
    *    This method will return the first matching user.
    * </p>
    * @param username The username.
    * @return The user or {@code null} if not found.
    * @throws SQLException on database error.
    */
   public User selectUser(final String username) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectUserTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserByUsernameSQL);
         stmt.setString(1, username);
         rs = stmt.executeQuery();
         return rs.next() ? userFromResultSet(rs) : null;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String selectUserByIdSQL = selectUserSQL + " WHERE ID=?";

   /**
    * Selects a user from the database.
    * @param  userId The user id.
    * @return The author or {@code null} if not found.
    * @throws SQLException on database error.
    */
   public User selectUser(final long userId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectUserTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserByIdSQL);
         stmt.setLong(1, userId);
         rs = stmt.executeQuery();
         return rs.next() ? userFromResultSet(rs) : null;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Resolves a user by id, possibly with the internal cache.
    * @param userId The user id.
    * @return The user or {@code null}, if not found.
    * @throws SQLException on database error.
    */
   public User resolveUser(final long userId) throws SQLException {
      userCacheTries.mark();
      User user = userCache.getIfPresent(userId);
      if(user != null) {
         userCacheHits.mark();
         return user;
      } else {
         user = selectUser(userId);
         if(user != null) {
            userCache.put(userId, user);
         }
         return user;
      }
   }

   /**
    * Resolves a user by username, possibly with the internal cache.
    * @param username The username.
    * @return The user or {@code null}, if not found.
    * @throws SQLException on database error.
    */
   public User resolveUser(final String username) throws SQLException {
      usernameCacheTries.mark();
      User user = usernameCache.getIfPresent(username);
      if(user != null) {
         usernameCacheHits.mark();
         return user;
      } else {
         user = selectUser(username);
         if(user != null) {
            usernameCache.put(username, user);
         }
         return user;
      }
   }

   private static final String selectUserMetaSQL = "SELECT umeta_id, meta_key, meta_value FROM wp_usermeta WHERE user_id=?";

   /**
    * Selects metadata for a user.
    * @param userId The user id.
    * @return The metadata.
    * @throws SQLException on database error.
    */
   public List<Meta> userMetadata(final long userId) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Meta> meta = Lists.newArrayListWithExpectedSize(16);
      Timer.Context ctx = userMetadataTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserMetaSQL);
         stmt.setLong(1, userId);
         rs = stmt.executeQuery();
         while(rs.next()) {
            meta.add(new Meta(rs.getLong(1), rs.getString(2), rs.getString(3)));
         }
         return meta;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private final String deletePostIdSQL;

   /**
    * Deletes a post with a specified id, including all associated metadata.
    * @param postId The post id.
    * @throws SQLException on database error.
    */
   public void deletePost(final long postId) throws SQLException {
      clearPostMeta(postId);
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = deletePostTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deletePostIdSQL);
         stmt.setLong(1, postId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private static final String selectPostSQL =
           "SELECT ID, post_author, post_date_gmt, post_content, post_title, post_excerpt, post_status, post_name, post_modified_gmt," +
                   "post_parent, guid, post_type, post_mime_type FROM ";

   /**
    * Builds a post from a result set.
    * @param rs The result set.
    * @return The post (builder).
    * @throws SQLException on database error.
    */
   private Post.Builder postFromResultSet(final ResultSet rs) throws SQLException {
      Post.Builder post = Post.newBuilder();
      post.setId(rs.getLong(1));
      post.setAuthorId(rs.getLong(2));
      post.setPublishTimestamp(rs.getTimestamp(3).getTime());
      post.setContent(Strings.emptyToNull(rs.getString(4)));
      post.setTitle(Strings.emptyToNull(rs.getString(5)));
      post.setExcerpt(Strings.emptyToNull(rs.getString(6)));
      post.setStatus(Post.Status.fromString(rs.getString(7)));
      post.setSlug(Strings.emptyToNull(rs.getString(8)));
      post.setModifiedTimestamp(rs.getTimestamp(9).getTime());
      post.setParentId(rs.getLong(10));
      post.setGUID(Strings.emptyToNull(rs.getString(11)));
      post.setType(Post.Type.fromString(rs.getString(12)));
      post.setMimeType(rs.getString(13));
      return post;
   }

   /**
    * Selects a page of posts for an author.
    * @param userId The user id for the author.
    * @param sort The sort direction.
    * @param paging The paging.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Post> selectAuthorPosts(final long userId,
                                       final Post.Sort sort,
                                       final Paging paging,
                                       final boolean withResolve) throws SQLException {

      if(paging.limit < 1 || paging.start < 0) {
         return ImmutableList.of();
      }

      List<Post.Builder> builders = Lists.newArrayListWithExpectedSize(paging.limit < 1024 ? paging.limit : 1024);

      StringBuilder sql = new StringBuilder(selectPostSQL);
      sql.append(postsTableName);
      sql.append(" WHERE post_author=?");
      appendPagingSortSQL(sql, sort, paging);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectAuthorPostsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setLong(1, userId);
         if(paging.interval != null) {
            stmt.setTimestamp(2, new Timestamp(paging.interval.getStartMillis()));
            stmt.setTimestamp(3, new Timestamp(paging.interval.getStartMillis()));
            stmt.setInt(4, paging.start);
            stmt.setInt(5, paging.limit);
         } else {
            stmt.setInt(2, paging.start);
            stmt.setInt(3, paging.limit);
         }
         rs = stmt.executeQuery();
         while(rs.next()) {
            builders.add(postFromResultSet(rs));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Post> posts = Lists.newArrayListWithExpectedSize(builders.size());
      for(Post.Builder builder : builders) {
         if(withResolve) {
            posts.add(resolve(builder).build());
         } else {
            posts.add(builder.build());
         }
      }

      return posts;
   }

   /**
    * Selects a page of posts with a specified type.
    * @param type The post type.
    * @param status The required post status.
    * @param sort The page sort.
    * @param paging The page range and interval.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Post> selectPosts(final Post.Type type,
                                 final Post.Status status,
                                 final Post.Sort sort,
                                 final Paging paging,
                                 final boolean withResolve) throws SQLException {

      if(paging.limit < 1 || paging.start < 0) {
         return ImmutableList.of();
      }

      List<Post.Builder> builders = Lists.newArrayListWithExpectedSize(paging.limit < 1024 ? paging.limit : 1024);

      StringBuilder sql = new StringBuilder(selectPostSQL);
      sql.append(postsTableName);
      sql.append(" WHERE post_type=? AND post_status=?");
      appendPagingSortSQL(sql, sort, paging);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectPostsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setString(1, type.toString().toLowerCase());
         stmt.setString(2, status.toString().toLowerCase());
         if(paging.interval != null) {
            stmt.setTimestamp(3, new Timestamp(paging.interval.getStartMillis()));
            stmt.setTimestamp(4, new Timestamp(paging.interval.getEndMillis()));
            stmt.setInt(5, paging.start);
            stmt.setInt(6, paging.limit);
         } else {
            stmt.setInt(3, paging.start);
            stmt.setInt(4, paging.limit);
         }
         rs = stmt.executeQuery();
         while(rs.next()) {
            builders.add(postFromResultSet(rs));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Post> posts = Lists.newArrayListWithExpectedSize(builders.size());
      for(Post.Builder builder : builders) {
         if(withResolve) {
            posts.add(resolve(builder).build());
         } else {
            posts.add(builder.build());
         }
      }

      return posts;
   }

   /**
    * Selects a page of posts with a specified type.
    * @param type The post type.
    * @param status The required post status.
    * @param terms A collection of terms attached to the posts.
    * @param sort The page sort.
    * @param paging The page range and interval.
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Long> selectPostIds(final Post.Type type,
                                   final Post.Status status,
                                   final Collection<TaxonomyTerm> terms,
                                   final Post.Sort sort,
                                   final Paging paging) throws SQLException {

      if(paging.limit < 1 || paging.start < 0) {
         return ImmutableList.of();
      }

      List<Long> ids = Lists.newArrayListWithExpectedSize(paging.limit < 1024 ? paging.limit : 1024);

      StringBuilder sql = new StringBuilder("SELECT ID FROM ");
      sql.append(postsTableName);
      if(terms != null && terms.size() > 0) {
         sql.append(",").append(termRelationshipsTableName);
         sql.append(" WHERE post_type=? AND post_status=? AND object_id=ID AND ");
         if(terms.size() == 1) {
            sql.append("term_taxonomy_id=").append(terms.iterator().next().id);
         } else {
            sql.append("term_taxonomy_id IN (");
            Iterator<TaxonomyTerm> iter = terms.iterator();
            sql.append(iter.next().id);
            while(iter.hasNext()) {
               sql.append(",").append(iter.next().id);
            }
            sql.append(")");
         }
      } else {
         sql.append(" WHERE post_type=? AND post_status=?");
      }
      appendPagingSortSQL(sql, sort, paging);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectPostIdsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setString(1, type.toString().toLowerCase());
         stmt.setString(2, status.toString().toLowerCase());
         if(paging.interval != null) {
            stmt.setTimestamp(3, new Timestamp(paging.interval.getStartMillis()));
            stmt.setTimestamp(4, new Timestamp(paging.interval.getEndMillis()));
            stmt.setInt(5, paging.start);
            stmt.setInt(6, paging.limit);
         } else {
            stmt.setInt(3, paging.start);
            stmt.setInt(4, paging.limit);
         }
         rs = stmt.executeQuery();
         while(rs.next()) {
            ids.add(rs.getLong(1));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
      return ids;
   }

   private final String selectChildrenSQL;

   /**
    * Gets all children for a post.
    * @param parentId The parent post id.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of children.
    * @throws SQLException on database error.
    */
   public List<Post> selectChildren(final long parentId, final boolean withResolve) throws SQLException {

      List<Post.Builder> builders = Lists.newArrayListWithExpectedSize(4);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectChildrenTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectChildrenSQL);
         stmt.setLong(1, parentId);
         rs = stmt.executeQuery();
         while(rs.next()) {
            builders.add(postFromResultSet(rs));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Post> posts = Lists.newArrayListWithExpectedSize(builders.size());
      for(Post.Builder builder : builders) {
         if(withResolve) {
            posts.add(resolve(builder).build());
         } else {
            posts.add(builder.build());
         }
      }

      return posts;
   }

   /**
    * Appends paging interval constraint, if required, paging and sort.
    * @param sql The buffer to append to.
    * @param sort The sort.
    * @param paging The paging.
    */
   private void appendPagingSortSQL(final StringBuilder sql, final Post.Sort sort,final Paging paging) {

      if(paging.interval != null) {
         sql.append(" AND post_date");
         sql.append(paging.startIsOpen ? " >" : " >=");
         sql.append("?");

         sql.append(" AND post_date");
         sql.append(paging.endIsOpen ? " <" : " <=");
         sql.append("?");
      }

      switch(sort) {
         case ASC:
            sql.append(" ORDER BY post_date ASC");
            break;
         case DESC:
            sql.append(" ORDER BY post_date DESC");
            break;
         case ASC_MOD:
            sql.append(" ORDER BY post_modified ASC");
            break;
         case DESC_MOD:
            sql.append(" ORDER BY post_modified DESC");
            break;
         case ID_ASC:
            sql.append(" ORDER BY ID ASC");
            break;
         case ID_DESC:
            sql.append(" ORDER BY ID DESC");
            break;
         default:
            sql.append(" ORDER BY post_date DESC");
            break;
      }
      sql.append(" LIMIT ?,?");
   }

   private final String selectPostsBySlugSQL;

   /**
    * Selects all posts with a "slug".
    * <p>
    *    Ideally there should be just one, but this is not guaranteed
    *    by the database.
    * </p>
    * @param slug The slug.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of matching posts.
    * @throws SQLException on database error.
    */
   public List<Post> selectPosts(final String slug, final boolean withResolve) throws SQLException {

      List<Post.Builder> builders = Lists.newArrayListWithExpectedSize(2);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectSlugPostsTimer.time();

      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectPostsBySlugSQL);
         stmt.setString(1, slug);
         rs = stmt.executeQuery();
         while(rs.next()) {
            builders.add(postFromResultSet(rs));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Post> posts = Lists.newArrayListWithExpectedSize(builders.size());
      for(Post.Builder builder : builders) {
         if(withResolve) {
            posts.add(resolve(builder).build());
         } else {
            posts.add(builder.build());
         }
      }
      return posts;
   }


   /**
    * Resolves user, author, terms and meta for a post.
    * @param post The post builder.
    * @return The builder with resolved items.
    * @throws SQLException on database error.
    */
   public Post.Builder resolve(final Post.Builder post) throws SQLException {
      Timer.Context ctx = resolvePostTimer.time();
      try {
         User author = resolveUser(post.getAuthorId());
         if(author != null) {
            List<Meta> meta = userMetadata(post.getAuthorId());
            if(meta.size() > 0) {
               author = author.withMetadata(meta);
            }
            post.setAuthor(author);
         }

         List<Meta> meta = selectPostMeta(post.getId());
         if(meta.size() > 0) {
            post.setMetadata(meta);
         }

         List<TaxonomyTerm> terms = selectPostTerms(post.getId());
         if(terms.size() > 0) {
            post.setTaxonomyTerms(terms);
         }

         List<Post> children = selectChildren(post.getId(), false);
         if(children.size() > 0) {
            post.setChildren(children);
         }
         return post;
      } finally {
         ctx.stop();
      }
   }

   /**
    * Selects a post by id.
    * @param postId The post id.
    * @return The post.
    * @throws SQLException on database error.
    */
   public Post.Builder selectPost(final long postId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectPostTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectPostSQL + postsTableName + " WHERE ID=?");
         stmt.setLong(1, postId);
         rs = stmt.executeQuery();
         return rs.next() ? postFromResultSet(rs) : null;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private final String insertPostWithIdSQL;
   private final String insertPostSQL;

   /**
    * Inserts a post.
    *
    * <p>
    *    If the post has a non-zero {@code id}, it will be inserted with this id,
    *    otherwise an id will be generated.
    * </p>
    * @param post The post.
    * @param tz The local time zone for the post.
    * @return The post with generated id.
    * @throws SQLException on database error or post with duplicate id.
    */
   public Post insertPost(final Post post, final TimeZone tz) throws SQLException {
      if(post.id > 0) {
         return insertPostWithId(post, tz);
      }

      int offset = tz.getOffset(post.publishTimestamp);
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = insertPostTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertPostSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setLong(1, post.authorId);
         stmt.setTimestamp(2, new Timestamp(post.publishTimestamp));
         stmt.setTimestamp(3, new Timestamp(post.publishTimestamp - offset));
         stmt.setString(4, Strings.nullToEmpty(post.content));
         stmt.setString(5, Strings.nullToEmpty(post.title));
         stmt.setString(6, Strings.nullToEmpty(post.excerpt));
         stmt.setString(7, post.status.toString().toLowerCase());
         stmt.setString(8, Strings.nullToEmpty(post.slug));
         stmt.setTimestamp(9, new Timestamp(post.modifiedTimestamp));
         stmt.setTimestamp(10, new Timestamp(post.modifiedTimestamp - offset));
         stmt.setLong(11, post.parentId);
         stmt.setString(12, Strings.nullToEmpty(post.guid));
         stmt.setString(13, "post");
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return post.withId(rs.getLong(1));
         } else {
            throw new SQLException("Problem creating post (no generated id)");
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private Post insertPostWithId(final Post post, final TimeZone tz) throws SQLException {
      int offset = tz.getOffset(post.publishTimestamp);
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = insertPostTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertPostWithIdSQL);
         stmt.setLong(1, post.id);
         stmt.setLong(2, post.authorId);
         stmt.setTimestamp(3, new Timestamp(post.publishTimestamp));
         stmt.setTimestamp(4, new Timestamp(post.publishTimestamp - offset));
         stmt.setString(5, Strings.nullToEmpty(post.content));
         stmt.setString(6, Strings.nullToEmpty(post.title));
         stmt.setString(7, Strings.nullToEmpty(post.excerpt));
         stmt.setString(8, post.status.toString().toLowerCase());
         stmt.setString(9, Strings.nullToEmpty(post.slug));
         stmt.setTimestamp(10, new Timestamp(post.modifiedTimestamp));
         stmt.setTimestamp(11, new Timestamp(post.modifiedTimestamp - offset));
         stmt.setLong(12, post.parentId);
         stmt.setString(13, Strings.nullToEmpty(post.guid));
         stmt.setString(14, "post");
         stmt.executeUpdate();
         return post;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private final String deletePostMetaSQL;

   /**
    * Clears all metadata for a post.
    * @param postId The post id.
    * @throws SQLException on database error.
    */
   public void clearPostMeta(final long postId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = clearPostMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deletePostMetaSQL);
         stmt.setLong(1, postId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private final String selectPostMetaSQL;

   /**
    * Selects metadata for a post.
    * @param postId The post id.
    * @return The metadata.
    * @throws SQLException on database error.
    */
   public List<Meta> selectPostMeta(final long postId) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Meta> meta = Lists.newArrayListWithExpectedSize(8);
      Timer.Context ctx = selectPostMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectPostMetaSQL);
         stmt.setLong(1, postId);
         rs = stmt.executeQuery();
         while(rs.next()) {
            meta.add(new Meta(rs.getLong(1), rs.getString(2), rs.getString(3)));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      return meta;
   }

   final String insertPostMetaSQL;

   /**
    * Sets metadata for a post.
    * <p>
    *    Clears existing metadata.
    * </p>
    * @param postId The post id.
    * @param postMeta The metadata.
    * @throws SQLException on database error.
    */
   public void setPostMeta(final long postId, final List<Meta> postMeta) throws SQLException {

      clearPostMeta(postId);

      if(postMeta == null || postMeta.size() == 0) {
         return;
      }
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = setPostMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertPostMetaSQL);
         for(Meta meta : postMeta) {
            stmt.setLong(1, postId);
            stmt.setString(2, meta.key);
            stmt.setString(3, meta.value);
            stmt.executeUpdate();
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private final String selectTermIdsSQL;

   /**
    * Selects the term ids for all with the specified name.
    * @param name The term name.
    * @return The list of ids.
    * @throws SQLException on database error.
    */
   public Set<Long> selectTermIds(final String name) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Set<Long> ids = Sets.newHashSetWithExpectedSize(4);
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTermIdsSQL);
         stmt.setString(1, name);
         rs = stmt.executeQuery();
         while(rs.next()) {
            ids.add(rs.getLong(1));
         }
         return ids;
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private final String insertTermSQL;

   /**
    * Creates a term.
    * @param name The term name.
    * @param slug The term slug.
    * @return The created term.
    * @throws SQLException on database error.
    */
   public Term createTerm(final String name, final String slug) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = createTermTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertTermSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setString(1, name);
         stmt.setString(2, slug);
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return new Term(rs.getLong(1), name, slug);
         } else {
            throw new SQLException("Problem creating term (no generated id)");
         }
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt, rs);
      }
   }

   private final String selectTermIdSQL;

   /**
    * Selects a term by id.
    * @param id The id.
    * @return The term or {@code null} if none.
    * @throws SQLException on database error.
    */
   public Term selectTerm(final long id) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = selectTermTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTermIdSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         return rs.next() ? new Term(id, rs.getString(1), rs.getString(2)) : null;
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt, rs);
      }
   }

   private final String selectTaxonomyTermSQL;

   /**
    * Selects a taxonomy term.
    * @param taxonomy The taxonomy name.
    * @param name The term name.
    * @return The taxonomy term or {@code null} if none.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm selectTaxonomyTerm(final String taxonomy, final String name) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      long taxonomyTermId = 0L;
      long termId = 0L;
      String description = "";
      Timer.Context ctx = selectTaxonomyTermTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTaxonomyTermSQL);
         stmt.setString(1, name);
         stmt.setString(2, taxonomy);
         rs = stmt.executeQuery();
         if(rs.next()) {
            taxonomyTermId = rs.getLong(1);
            termId = rs.getLong(2);
            description = rs.getString(3);
         } else {
            return null;
         }
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt, rs);
      }
      return new TaxonomyTerm(taxonomyTermId, taxonomy, selectTerm(termId), description);
   }

   final String insertTaxonomyTermSQL;

   /**
    * Creates a taxonomy term.
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @param slug The term slug.
    * @param description The taxonomy term description.
    * @return The created term.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm createTaxonomyTerm(final String taxonomy, final String name, final String slug,
                                          final String description) throws SQLException {
      Term term = createTerm(name, slug);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = createTaxonomyTermTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertTaxonomyTermSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setLong(1, term.id);
         stmt.setString(2, taxonomy);
         stmt.setString(3, Strings.nullToEmpty(description));
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return new TaxonomyTerm(rs.getLong(1), taxonomy, term, description);
         } else {
            throw new SQLException("Problem creating taxonomy term (no generated id)");
         }
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Resolves a taxonomy term, creating if required.
    * <p>
    *    If taxonomy term cache is configured for this taxonomy, it
    *    is used for resolution.
    * </p>
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @return The taxonomy term.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm resolveTaxonomyTerm(final String taxonomy, final String name) throws SQLException {

      TaxonomyTerm term;
      Cache<String, TaxonomyTerm> taxonomyTermCache = taxonomyTermCaches.get(taxonomy);
      if(taxonomyTermCache != null) {
         taxonomyTermCacheTries.mark();
         term = taxonomyTermCache.getIfPresent(name);
         if(term != null) {
            taxonomyTermCacheHits.mark();
            return term;
         }
      }

      term = selectTaxonomyTerm(taxonomy, name);
      if(term == null) {
         term = createTaxonomyTerm(taxonomy, name, slugify(name), "");
      }

      if(taxonomyTermCache != null) {
         taxonomyTermCache.put(name, term);
      }
      return term;
   }

   private final String selectTaxonomyTermIdSQL;

   /**
    * Resolves a taxonomy term by id.
    * <p>
    *    Uses configured caches.
    * </p>
    * @param id The taxonomy term id.
    * @return The resolved term or {@code null} if not found.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm resolveTaxonomyTerm(final long id) throws SQLException {

      taxonomyTermCacheTries.mark();
      TaxonomyTerm term = taxonomyTermCache.getIfPresent(id);
      if(term != null) {
         taxonomyTermCacheHits.mark();
         return term;
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      long termId = 0;
      String taxonomy = "";
      String description = "";
      Timer.Context ctx = taxonomyTermResolveTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTaxonomyTermIdSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         if(rs.next()) {
            taxonomy = rs.getString(1);
            termId = rs.getLong(2);
            description = rs.getString(3);
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      if(termId > 0) {
         term = new TaxonomyTerm(id, taxonomy, selectTerm(termId), description);
         taxonomyTermCache.put(id, term);
         return term;
      } else {
         return null;
      }
   }

   private final String clearPostTermSQL;

   /**
    * Clears a single taxonomy term associated with a post.
    * @param postId The post id.
    * @param taxonomyTermId The taxonomy term id.
    * @throws SQLException on database error.
    */
   public void clearPostTerm(final long postId, final long taxonomyTermId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = postTermsClearTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(clearPostTermSQL);
         stmt.setLong(1, postId);
         stmt.setLong(2, taxonomyTermId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private final String clearPostTermsSQL;

   /**
    * Clears all terms associated with a post.
    * @param postId The post id.
    * @throws SQLException on database error.
    */
   public void clearPostTerms(final long postId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = postTermsClearTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(clearPostTermsSQL);
         stmt.setLong(1, postId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   /**
    * Clears all terms associated with a post with a specified taxonomy.
    * @param postId The post id.
    * @param taxonomy The taxonomy.
    * @throws SQLException on database error.
    */
   public void clearPostTerms(final long postId, final String taxonomy) throws SQLException {
      List<TaxonomyTerm> terms = selectPostTerms(postId, taxonomy);
      for(TaxonomyTerm term : terms) {
         clearPostTerm(postId, term.id);
      }
   }

   private final String insertPostTermSQL;

   /**
    * Sets terms associated with a post, replacing any existing terms with the specified taxonomy.
    * <p>
    *    Uses cache, if configured, to resolve.
    * </p>
    * @param postId The post id.
    * @param taxonomy The taxonomy.
    * @param terms A list of term names.
    * @return The list of taxonomy terms.
    * @throws SQLException on database error.
    */
   public List<TaxonomyTerm> setPostTerms(final long postId, final String taxonomy, final List<String> terms) throws SQLException {
      clearPostTerms(postId, taxonomy);
      if(terms == null || terms.size() == 0) {
         return ImmutableList.of();
      }

      List<TaxonomyTerm> taxonomyTerms = Lists.newArrayListWithExpectedSize(terms.size());
      for(String term : terms) {
         taxonomyTerms.add(resolveTaxonomyTerm(taxonomy, term));
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = postTermsSetTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertPostTermSQL);
         int pos = 0;
         for(TaxonomyTerm taxonomyTerm : taxonomyTerms) {
            stmt.setLong(1, postId);
            stmt.setLong(2, taxonomyTerm.id);
            stmt.setInt(3, pos++);
            stmt.executeUpdate();
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }

      return taxonomyTerms;
   }

   public final String selectPostTermsSQL;

   /**
    * Selects all terms associated with a post for any taxonomy.
    * @param postId The post id.
    * @return The list of terms.
    * @throws SQLException on database error.
    */
   public List<TaxonomyTerm> selectPostTerms(final long postId) throws SQLException {
      return selectPostTerms(postId, null);
   }


   /**
    * Selects all terms associated with a post.
    * @param postId The post id.
    * @param taxonomy The taxonomy. If {@code null}, any taxonomy is accepted.
    * @return The list of terms.
    * @throws SQLException on database error.
    */
   public List<TaxonomyTerm> selectPostTerms(final long postId, final String taxonomy) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Long> termIds = Lists.newArrayListWithExpectedSize(8);
      Timer.Context ctx = postTermsSelectTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectPostTermsSQL);
         stmt.setLong(1, postId);
         rs = stmt.executeQuery();
         while(rs.next()) {
            termIds.add(rs.getLong(1));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      if(termIds.size() == 0) {
         return ImmutableList.of();
      }

      List<TaxonomyTerm> terms = Lists.newArrayListWithExpectedSize(termIds.size());
      for(long termId : termIds) {
         TaxonomyTerm term = resolveTaxonomyTerm(termId);
         if(term != null && (taxonomy == null || term.taxonomy.equals(taxonomy))) {
            terms.add(term);
         }
      }
      return terms;
   }

   private final String selectOptionSQL;


   /**
    * Gets a configuration option.
    * @param optionName The option name.
    * @return The option value or {@code null} if not found.
    * @throws SQLException on database error.
    */
   public String selectOption(final String optionName) throws SQLException {
      return selectOption(optionName, null);
   }

   /**
    * Gets a configuration option with a default value.
    * @param optionName The option name.
    * @param defaultValue A default value if no option is set.
    * @return The option value or the default value if not found.
    * @throws SQLException on database error.
    */
   public String selectOption(final String optionName, final String defaultValue) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = optionSelectTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectOptionSQL);
         stmt.setString(1, optionName);
         rs = stmt.executeQuery();
         if(rs.next()) {
            String val = rs.getString(1);
            return val != null ? val.trim() : defaultValue;
         } else {
            return defaultValue;
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Selects the site metadata from the options table.
    * @return The site metadata.
    * @throws SQLException on database error.
    */
   public Site selectSite() throws SQLException {
      String baseURL = selectOption("home");
      String title = selectOption("blogname");
      String description = selectOption("blogdescription");
      String permalinkStructure = selectOption("permalink_structure", "/?p=%postid%");
      long defaultCategoryId = Long.parseLong(selectOption("default_category", "0"));
      TaxonomyTerm defaultCategoryTerm = resolveTaxonomyTerm(defaultCategoryId);
      if(defaultCategoryTerm == null) {
         defaultCategoryTerm = new TaxonomyTerm(0L, CATEGORY_TAXONOMY, new Term(0L, "Uncategorized", "uncategorized"), "");
      }
      return new Site(siteId, baseURL, title, description, permalinkStructure, defaultCategoryTerm.term);
   }

   private final Timer optionSelectTimer = new Timer();
   private final Timer postTermsSelectTimer = new Timer();
   private final Timer postTermsSetTimer = new Timer();
   private final Timer postTermsClearTimer = new Timer();
   private final Timer taxonomyTermResolveTimer = new Timer();
   private final Timer selectTaxonomyTermTimer = new Timer();
   private final Timer createTaxonomyTermTimer = new Timer();
   private final Timer createUserTimer = new Timer();
   private final Timer selectUserTimer = new Timer();
   private final Timer userMetadataTimer = new Timer();
   private final Timer deletePostTimer = new Timer();
   private final Timer selectAuthorPostsTimer = new Timer();
   private final Timer selectPostsTimer = new Timer();
   private final Timer selectPostIdsTimer = new Timer();
   private final Timer selectChildrenTimer = new Timer();
   private final Timer selectSlugPostsTimer = new Timer();
   private final Timer selectPostTimer = new Timer();
   private final Timer insertPostTimer = new Timer();
   private final Timer clearPostMetaTimer = new Timer();
   private final Timer selectPostMetaTimer = new Timer();
   private final Timer setPostMetaTimer = new Timer();
   private final Timer createTermTimer = new Timer();
   private final Timer selectTermTimer = new Timer();
   private final Timer resolvePostTimer = new Timer();

   private final Meter userCacheHits = new Meter();
   private final Meter userCacheTries = new Meter();

   private final Meter usernameCacheHits = new Meter();
   private final Meter usernameCacheTries = new Meter();

   private final Meter taxonomyTermCacheHits = new Meter();
   private final Meter taxonomyTermCacheTries = new Meter();

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>builder()
              .put("select-option", optionSelectTimer)
              .put("select-post-terms", postTermsSelectTimer)
              .put("set-post-terms", postTermsSetTimer)
              .put("clear-post-terms", postTermsClearTimer)
              .put("resolve-taxonomy-term", taxonomyTermResolveTimer)
              .put("select-taxonomy-term", selectTaxonomyTermTimer)
              .put("create-taxonomy-term", createTaxonomyTermTimer)
              .put("create-user", createUserTimer)
              .put("select-user", selectUserTimer)
              .put("select-user-metadata", userMetadataTimer)
              .put("delete-post", deletePostTimer)
              .put("select-author-posts", selectAuthorPostsTimer)
              .put("select-posts", selectPostsTimer)
              .put("select-post-ids", selectPostIdsTimer)
              .put("select-post-children", selectChildrenTimer)
              .put("select-slug-post", selectSlugPostsTimer)
              .put("select-post", selectPostTimer)
              .put("insert-post", insertPostTimer)
              .put("resolve-post", resolvePostTimer)
              .put("set-post-meta", setPostMetaTimer)
              .put("clear-post-meta", clearPostMetaTimer)
              .put("select-post-meta", selectPostMetaTimer)
              .put("create-term", createTermTimer)
              .put("select-term", selectTermTimer)
              .put("try-user-cache", userCacheTries)
              .put("hit-user-cache", userCacheHits)
              .put("try-username-cache", usernameCacheTries)
              .put("hit-username-cache", usernameCacheHits)
              .put("try-taxonomy-term-cache", taxonomyTermCacheTries)
              .put("hit-taxonomy-term-cache", taxonomyTermCacheHits)
              .build();
   }

   /**
    * The site id.
    */
   public final long siteId;

   private final ConnectionSupplier connectionSupplier;
   private final String postsTableName;
   private final String termRelationshipsTableName;
   private final Cache<Long, User> userCache;
   private final Cache<String, User> usernameCache;
   private final ImmutableMap<String, Cache<String, TaxonomyTerm>> taxonomyTermCaches;
   private final Cache<Long, TaxonomyTerm> taxonomyTermCache;
}
