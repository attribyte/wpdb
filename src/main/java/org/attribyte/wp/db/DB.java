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


import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.attribyte.sql.ConnectionSupplier;
import org.attribyte.util.SQLUtil;
import org.attribyte.wp.model.Blog;
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
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.attribyte.util.SQLUtil.closeQuietly;
import static org.attribyte.wp.Util.CATEGORY_TAXONOMY;
import static org.attribyte.wp.Util.slugify;

public class DB implements MetricSet {

   /**
    * Creates a database with the default metric source.
    * @param connectionSupplier Supplies connections to the underlying database.
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
      this(connectionSupplier, siteId, cachedTaxonomies, taxonomyCacheTimeout, userCacheTimeout, MetricSource.DEFAULT);
   }

   /**
    * Creates a database for another site with shared user caches, metrics and taxonomy terms.
    * @param siteId The site id.
    * @return The site-specific database.
    */
   public DB forSite(final long siteId) {
      return siteId == this.siteId ? this : new DB(this.connectionSupplier, siteId, this.taxonomyTermCaches.keySet(), this.taxonomyCacheTimeout,
              this.userCache, this.usernameCache, this.metrics);
   }

   /**
    * Creates a database.
    * @param connectionSupplier Supplies connections to the underlying database.
    * @param siteId The site id.
    * @param cachedTaxonomies Enable caches for these taxonomies.
    * @param taxonomyCacheTimeout The expiration for taxonomy caches. If {@code 0}, caching is disabled.
    * @param userCacheTimeout The expiration for user caches. If {@code 0}, caching is disabled.
    * @param metricSource A metric source.
    */
   public DB(final ConnectionSupplier connectionSupplier,
             final long siteId,
             final Set<String> cachedTaxonomies,
             final Duration taxonomyCacheTimeout,
             final Duration userCacheTimeout,
             final MetricSource metricSource) {
      this(connectionSupplier, siteId, cachedTaxonomies, taxonomyCacheTimeout,
              CacheBuilder.newBuilder()
                      .concurrencyLevel(4)
                      .expireAfterWrite(userCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
                      .build(),
              CacheBuilder.newBuilder()
                      .concurrencyLevel(4)
                      .expireAfterWrite(userCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
                      .build(),
              new Metrics(metricSource));
   }

   /**
    * Creates a database.
    * @param connectionSupplier Supplies connections to the underlying database.
    * @param siteId The site id.
    * @param cachedTaxonomies Enable caches for these taxonomies.
    * @param taxonomyCacheTimeout The expiration for taxonomy caches. If {@code 0}, caching is disabled.
    * @param userCache A cache of user vs id.
    * @param usernameCache A cache of user vs username.
    * @param metrics The metrics.
    */
   public DB(final ConnectionSupplier connectionSupplier,
             final long siteId,
             final Set<String> cachedTaxonomies,
             final Duration taxonomyCacheTimeout,
             final Cache<Long, User> userCache,
             final Cache<String, User> usernameCache,
             final Metrics metrics) {
      this.connectionSupplier = connectionSupplier;
      this.siteId = siteId;

      final String postMetaTableName;
      final String termTaxonomyTableName;
      final String termsTableName;
      final String termsMetaTableName;
      final String optionsTableName;

      if(siteId < 2) {
         this.postsTableName = "wp_posts";
         postMetaTableName = "wp_postmeta";
         optionsTableName = "wp_options";
         termsTableName = "wp_terms";
         termsMetaTableName = "wp_termmeta";
         termRelationshipsTableName = "wp_term_relationships";
         termTaxonomyTableName = "wp_term_taxonomy";
      } else {
         this.postsTableName = "wp_" + siteId + "_posts";
         postMetaTableName = "wp_" + siteId + "_postmeta";
         optionsTableName = "wp_" + siteId + "_options";
         termsTableName = "wp_" + siteId + "_terms";
         termsMetaTableName = "wp_" + siteId + "_termmeta";
         termRelationshipsTableName = "wp_" + siteId + "_term_relationships";
         termTaxonomyTableName = "wp_" + siteId + "_term_taxonomy";
      }

      this.userCache = userCache;
      this.usernameCache = usernameCache;

      this.taxonomyCacheTimeout = taxonomyCacheTimeout;
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

      this.updateTaxonomyTermDescriptionSQL = "UPDATE " + termTaxonomyTableName + " SET description=? WHERE term_id=? AND taxonomy=?";

      this.clearPostTermsSQL = "DELETE FROM " + termRelationshipsTableName + " WHERE object_id=?";

      this.clearPostTermSQL = "DELETE FROM " + termRelationshipsTableName + " WHERE object_id=? AND term_taxonomy_id=?";

      this.insertPostTermSQL = "INSERT IGNORE INTO " + termRelationshipsTableName + " (object_id, term_taxonomy_id, term_order) VALUES (?,?,?)";

      this.selectPostTermsSQL = "SELECT term_taxonomy_id FROM " + termRelationshipsTableName + " WHERE object_id=? ORDER BY term_order ASC";

      this.selectTermMetaSQL = "SELECT meta_id, meta_key, meta_value FROM " + termsMetaTableName + " WHERE term_id=?";

      this.insertTermMetaSQL = "INSERT INTO " + termsMetaTableName + "(term_id, meta_key, meta_value) VALUES (?,?,?)";

      this.deleteTermMetaSQL = "DELETE FROM " + termsMetaTableName + " WHERE term_id=?";

      this.selectPostMetaSQL = "SELECT meta_id, meta_key, meta_value FROM " + postMetaTableName + " WHERE post_id=?";

      this.insertPostMetaSQL = "INSERT INTO " + postMetaTableName + "(post_id, meta_key, meta_value) VALUES (?,?,?)";

      this.deletePostMetaSQL = "DELETE FROM " + postMetaTableName + " WHERE post_id=?";

      this.selectOptionSQL = "SELECT option_value FROM " + optionsTableName + " WHERE option_name=?";

      this.selectPostsBySlugSQL = selectPostSQL + this.postsTableName + " WHERE post_name=? ORDER BY ID DESC";

      this.selectChildrenSQL = selectPostSQL + this.postsTableName + " WHERE post_parent=? ORDER BY ID ASC";

      this. deleteChildrenSQL = "DELETE FROM " + this.postsTableName + " WHERE post_parent=?";

      this.insertPostWithIdSQL = "INSERT INTO " + postsTableName +
              " (ID, post_author, post_date, post_date_gmt, post_content, post_title, " +
                      "post_excerpt, post_status, post_name, post_modified, post_modified_gmt," +
                      "post_parent, guid, post_type, to_ping, pinged, post_content_filtered, post_mime_type) VALUES " +
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?,?, '','', '',?)";

      this.insertPostSQL = "INSERT INTO " + postsTableName +
              " (post_author, post_date, post_date_gmt, post_content, post_title, " +
                      "post_excerpt, post_status, post_name, post_modified, post_modified_gmt," +
                      "post_parent, guid, post_type, to_ping, pinged, post_content_filtered, post_mime_type) VALUES " +
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?, '','', '',?)";

      this.updatePostSQL = "UPDATE " + postsTableName +
              " SET post_author=?, post_date=?, post_date_gmt=?, post_content=?, post_title=?, " +
              "post_excerpt=?, post_status=?, post_name=?, post_modified=?, post_modified_gmt=?, post_parent=?, " +
              "guid=?, post_type=?, post_mime_type=? WHERE ID=?";


      this.selectModPostsSQL = selectPostSQL + postsTableName +
              " WHERE post_modified > ? OR (post_modified=? AND ID > ?) ORDER BY post_modified ASC, ID ASC LIMIT ?";

      this.selectModPostsWithTypeSQL = selectPostSQL + postsTableName +
              " WHERE (post_modified > ? OR (post_modified=? AND ID > ?)) %s ORDER BY post_modified ASC, ID ASC LIMIT ?";

      this.metrics = metrics;
   }

   private static final String createUserSQL =
           "INSERT INTO wp_users (user_login, user_pass, user_nicename, display_name, user_email, user_registered) " +
                   "VALUES (?, ?, ?, ?, ?, NOW())";

   private static final String createUserWithIdSQL =
           "INSERT INTO wp_users (ID, user_login, user_pass, user_nicename, display_name, user_email, user_registered) " +
                   "VALUES (?, ?, ?, ?, ?, ?, NOW())";

   /**
    * Creates a user.
    * <p>
    *    If the {@code id} is > 0, the user will be created with this id, otherwise it
    *    will be auto-generated.
    * </p>
    * @param user The user.
    * @param userPass The {@code user_pass} string to use (probably the hash of a default username/password).
    * @return The newly created user.
    * @throws SQLException on database error.
    */
   public User createUser(final User user, final String userPass) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      String nicename = user.slug;
      if(nicename.length() > 49) {
         nicename = nicename.substring(0, 49);
      }

      String username = user.username.length() < 60 ? user.username : user.username.substring(0, 60);

      Timer.Context ctx = metrics.createUserTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         if(user.id > 0L) {
            stmt = conn.prepareStatement(createUserWithIdSQL);
            stmt.setLong(1, user.id);
            stmt.setString(2, username);
            stmt.setString(3, Strings.nullToEmpty(userPass));
            stmt.setString(4, nicename);
            stmt.setString(5, user.displayName());
            stmt.setString(6, Strings.nullToEmpty(user.email));
            stmt.executeUpdate();
            return user;
         } else {
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
      return new User(rs.getLong(1), rs.getString(2), useDisplayName, niceName, rs.getString(5), rs.getTimestamp(6).getTime(), ImmutableList.of());
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
      Timer.Context ctx = metrics.selectUserTimer.time();
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
      Timer.Context ctx = metrics.selectUserTimer.time();
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
      metrics.userCacheTries.mark();
      User user = userCache.getIfPresent(userId);
      if(user != null) {
         metrics.userCacheHits.mark();
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
      metrics.usernameCacheTries.mark();
      User user = usernameCache.getIfPresent(username);
      if(user != null) {
         metrics.usernameCacheHits.mark();
         return user;
      } else {
         user = selectUser(username);
         if(user != null) {
            usernameCache.put(username, user);
         }
         return user;
      }
   }

   private static final String deleteUserSQL = "DELETE FROM wp_users WHERE ID=?";

   /**
    * Deletes a user by id.
    * @param userId The user id.
    * @return Was the user deleted?
    * @throws SQLException on database error.
    */
   public boolean deleteUser(final long userId) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deleteUserSQL);
         stmt.setLong(1, userId);
         return stmt.executeUpdate() > 0;
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
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
      Timer.Context ctx = metrics.userMetadataTimer.time();
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

   private static final String selectUserMetaKeySQL = "SELECT umeta_id, meta_key, meta_value FROM wp_usermeta WHERE user_id=? AND meta_key=? ORDER BY umeta_id DESC";

   /**
    * Selects user metadata with a specified key.
    * @param userId The user id.
    * @param key The metadata key.
    * @return The list of values.
    * @throws SQLException on database error.
    */
   public List<Meta> userMetadata(final long userId, final String key) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Meta> meta = Lists.newArrayListWithExpectedSize(16);
      Timer.Context ctx = metrics.userMetadataTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserMetaKeySQL);
         stmt.setLong(1, userId);
         stmt.setString(2, key);
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

   private static final String deleteUserMetaSQL = "DELETE FROM wp_usermeta WHERE user_id=?";

   /**
    * Clears all metadata for a user.
    * @param userId The user id.
    * @throws SQLException on database error.
    */
   public void clearUserMeta(final long userId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.clearUserMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deleteUserMetaSQL);
         stmt.setLong(1, userId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private static final String firstUserMetaIdSQL = "SELECT umeta_id FROM wp_usermeta WHERE user_id=? AND meta_key=? ORDER BY umeta_id DESC LIMIT 1";
   private static final String insertUserMetaSQL = "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (?,?,?)";
   private static final String updateUserMetaSQL = "UPDATE wp_usermeta SET meta_value=? WHERE umeta_id=?";

   /**
    * Updates a user metadata value, replacing the value of the first existing match, or creating if none exists.
    * <p>
    *    Note that there is not a unique key, e.g. (user_id, meta_key), so users may have multiple metadata
    *    with the same name.
    * </p>
    * @param userId The user id.
    * @param key The metadata key.
    * @param value The metadata value.
    * @throws SQLException on database error.
    */
   public void updateUserMeta(final long userId, final String key, final String value) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(firstUserMetaIdSQL);
         stmt.setLong(1, userId);
         stmt.setString(2, key);
         rs = stmt.executeQuery();
         if(rs.next()) {
            long umetaId = rs.getLong(1);
            stmt = conn.prepareStatement(updateUserMetaSQL);
            stmt.setString(1, value);
            stmt.setLong(2, umetaId);
            stmt.executeUpdate();
         } else {
            SQLUtil.closeQuietly(stmt, rs);
            stmt = null;
            rs = null;
            stmt = conn.prepareStatement(insertUserMetaSQL);
            stmt.setLong(1, userId);
            stmt.setString(2, key);
            stmt.setString(3, value);
            stmt.executeUpdate();
         }
      } finally {
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
      Timer.Context ctx = metrics.deletePostTimer.time();
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
           "SELECT ID, post_author, post_date, post_content, post_title, post_excerpt, post_status, post_name, post_modified," +
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

      Timestamp ts = rs.getTimestamp(3);
      post.setPublishTimestamp(ts != null ? ts.getTime() : 0L);

      post.setContent(Strings.emptyToNull(rs.getString(4)));
      post.setTitle(Strings.emptyToNull(rs.getString(5)));
      post.setExcerpt(Strings.emptyToNull(rs.getString(6)));
      post.setStatus(Post.Status.fromString(rs.getString(7)));
      post.setSlug(Strings.emptyToNull(rs.getString(8)));

      ts = rs.getTimestamp(9);
      post.setModifiedTimestamp(ts != null ? ts.getTime() : 0L);

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
      Timer.Context ctx = metrics.selectAuthorPostsTimer.time();
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

   private final String selectModPostsSQL;
   private final String selectModPostsWithTypeSQL;

   /**
    * Selects recently modified posts, in ascending order after a specified timestamp and id.
    * @param type The post type. May be {@code null} for all types.
    * @param startTimestamp The timestamp after which posts were modified.
    * @param startId The start id. Posts that have timestamp that match {@code startTimestamp} exactly must if ids greater.
    * @param limit The maximum number of posts returned.
    * @return The list of posts.
    * @param withResolve Should associated users, etc be resolved?
    * @throws SQLException on database error.
    */
   public List<Post> selectModifiedPosts(final Post.Type type,
                                         final long startTimestamp,
                                         final long startId,
                                         final int limit,
                                         final boolean withResolve) throws SQLException {
      return selectModifiedPosts(type != null ? EnumSet.of(type) : null,
              startTimestamp, startId, limit, withResolve);
   }


   /**
    * Selects recently modified posts, in ascending order after a specified timestamp and id.
    * @param types The set of post type. May be {@code null} or empty for all types.
    * @param startTimestamp The timestamp after which posts were modified.
    * @param startId The start id. Posts that have timestamp that match {@code startTimestamp} exactly must if ids greater.
    * @param limit The maximum number of posts returned.
    * @return The list of posts.
    * @param withResolve Should associated users, etc be resolved?
    * @throws SQLException on database error.
    */
   public List<Post> selectModifiedPosts(final EnumSet<Post.Type> types,
                                         final long startTimestamp,
                                         final long startId,
                                         final int limit,
                                         final boolean withResolve) throws SQLException {

      List<Post.Builder> builders = Lists.newArrayListWithExpectedSize(limit < 1024 ? limit : 1024);
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = metrics.selectModPostsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         Timestamp ts = new Timestamp(startTimestamp);

         if(types == null || types.size() == 0) {
            stmt = conn.prepareStatement(selectModPostsSQL);
         } else {
            stmt = conn.prepareStatement(String.format(selectModPostsWithTypeSQL, appendPostTypes(types, new StringBuilder()).toString()));
         }

         stmt.setTimestamp(1, ts);
         stmt.setTimestamp(2, ts);
         stmt.setLong(3, startId);
         stmt.setInt(4, limit);

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
    * Selects a page of posts with a specific type.
    * @param type The post type. May be {@code null} for any type.
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
      return selectPosts(type != null ? EnumSet.of(type) : null, status, sort, paging, withResolve);
   }

   /**
    * Selects a page of posts with a set of specified types.
    * @param types The set of post types to be included. If {@code null} or empty, all types will be included.
    * @param status The required post status.
    * @param sort The page sort.
    * @param paging The page range and interval.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Post> selectPosts(final EnumSet<Post.Type> types,
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
      sql.append(" WHERE post_status=?");
      appendPostTypes(types, sql);
      appendPagingSortSQL(sql, sort, paging);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = metrics.selectPostsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setString(1, status.toString().toLowerCase());
         if(paging.interval != null) {
            stmt.setTimestamp(2, new Timestamp(paging.interval.getStartMillis()));
            stmt.setTimestamp(3, new Timestamp(paging.interval.getEndMillis()));
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
    * @param type The post type. May be {@code null} for any type.
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
      return selectPostIds(type != null ? EnumSet.of(type) : null, status, terms, sort, paging);
   }

   /**
    * Selects a page of posts with associated terms and a set of types.
    * @param types The post types. May be {@code null} or empty fo any type.
    * @param status The required post status.
    * @param terms A collection of terms attached to the posts.
    * @param sort The page sort.
    * @param paging The page range and interval.
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Long> selectPostIds(final EnumSet<Post.Type> types,
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
         sql.append(" WHERE post_status=? AND object_id=ID AND ");
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
         sql.append(" WHERE post_status=?");
      }

      appendPostTypes(types, sql);
      appendPagingSortSQL(sql, sort, paging);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = metrics.selectPostIdsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setString(1, status.toString().toLowerCase());
         if(paging.interval != null) {
            stmt.setTimestamp(2, new Timestamp(paging.interval.getStartMillis()));
            stmt.setTimestamp(3, new Timestamp(paging.interval.getEndMillis()));
            stmt.setInt(4, paging.start);
            stmt.setInt(5, paging.limit);
         } else {
            stmt.setInt(2, paging.start);
            stmt.setInt(3, paging.limit);
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

   private final String deleteChildrenSQL;

   /**
    * Deletes all children.
    * @param parentId The parent post id.
    * @throws SQLException on database error.
    */
   public void deleteChildren(final long parentId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.deleteChildrenTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deleteChildrenSQL);
         stmt.setLong(1, parentId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
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
      Timer.Context ctx = metrics.selectChildrenTimer.time();
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
    * Appends post type constraint.
    * @param types The types.
    * @param sql The buffer to append to.
    * @return The input buffer.
    */
   private StringBuilder appendPostTypes(final EnumSet<Post.Type> types, final StringBuilder sql) {
      int typesCount = types != null ? types.size() : 0;
      switch(typesCount) {
         case 0:
            break;
         case 1:
            sql.append(" AND post_type=").append(String.format("'%s'", types.iterator().next().toString()));
            break;
         default:
            sql.append(" AND post_type IN (");
            sql.append(inJoiner.join(types.stream().map(t -> String.format("'%s'", t.toString())).collect(Collectors.toSet())));
            sql.append(")");
            break;
      }

      return sql;
   }

   /**
    * Appends paging interval constraint, if required, paging and sort.
    * @param sql The buffer to append to.
    * @param sort The sort.
    * @param paging The paging.
    * @return The input buffer.
    */
   private StringBuilder appendPagingSortSQL(final StringBuilder sql, final Post.Sort sort,final Paging paging) {

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
      return sql;
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
      Timer.Context ctx = metrics.selectSlugPostsTimer.time();

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
      Timer.Context ctx = metrics.resolvePostTimer.time();
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
      Timer.Context ctx = metrics.selectPostTimer.time();
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


   /**
    * Selects posts from a collection of ids into a map.
    * @param postIds The collection of ids.
    * @param withResolve Should associated users, etc be resolved?
    * @return A map of post vs id.
    * @throws SQLException on database error.
    */
   public Map<Long, Post> selectPostMap(final Collection<Long> postIds, final boolean withResolve) throws SQLException {

      Map<Long, Post> postMap = Maps.newHashMapWithExpectedSize(postIds.size());

      Connection conn = null;
      Statement stmt = null;
      ResultSet rs = null;
      Timer.Context ctx = metrics.selectPostMapTimer.time();
      StringBuilder sql = new StringBuilder(selectPostSQL).append(postsTableName).append(" WHERE ID IN (");
      sql.append(inJoiner.join(postIds));
      sql.append(")");
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.createStatement();
         rs = stmt.executeQuery(sql.toString());
         while(rs.next()) {
            Post.Builder post = postFromResultSet(rs);
            postMap.put(post.getId(), withResolve ? resolve(post).build() : post.build());
         }
         return postMap;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Selects posts from a collection of ids into a list in input order.
    * @param postIds The collection of post ids.
    * @param withResolve Should associated users, etc be resolved?
    * @return The list of posts.
    * @throws SQLException on database error.
    */
   public List<Post> selectPosts(final Collection<Long> postIds, final boolean withResolve) throws SQLException {
      if(postIds == null || postIds.size() == 0) {
         return ImmutableList.of();
      }
      Map<Long, Post> postMap = selectPostMap(postIds, withResolve);
      List<Post> posts = Lists.newArrayListWithExpectedSize(postMap.size());
      for(long id : postIds) {
         Post post = postMap.get(id);
         if(post != null) {
            posts.add(post);
         }
      }
      return posts;
   }

   private final String updatePostSQL;

   /**
    * Updates a post.
    * @param post The post to update. The {@code id} must be set.
    * @param tz The local time zone.
    * @return The updated post.
    * @throws SQLException on database error or missing post id.
    */
   public Post updatePost(Post post, final TimeZone tz) throws SQLException {
      if(post.id < 1L) {
         throw new SQLException("The post id must be specified for update");
      }

      post = post.modifiedNow();

      int offset = tz.getOffset(post.publishTimestamp);
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.updatePostTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(updatePostSQL);
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
         stmt.setString(13, post.type.toString().toLowerCase());
         stmt.setString(14, post.mimeType != null ? post.mimeType : "");
         stmt.setLong(15, post.id);
         stmt.executeUpdate();
         return post;
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
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
      Timer.Context ctx = metrics.insertPostTimer.time();
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
         stmt.setString(13, post.type.toString().toLowerCase());
         stmt.setString(14, post.mimeType != null ? post.mimeType : "");
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
      Timer.Context ctx = metrics.insertPostTimer.time();
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
         stmt.setString(14, post.type.toString().toLowerCase());
         stmt.setString(15, post.mimeType != null ? post.mimeType : "");
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
      Timer.Context ctx = metrics.clearPostMetaTimer.time();
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
      Timer.Context ctx = metrics.selectPostMetaTimer.time();
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
      Timer.Context ctx = metrics.setPostMetaTimer.time();
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


   private final String deleteTermMetaSQL;

   /**
    * Clears all metadata for a term.
    * @param termId The term id.
    * @throws SQLException on database error.
    */
   public void clearTermMeta(final long termId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.clearTermMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(deleteTermMetaSQL);
         stmt.setLong(1, termId);
         stmt.executeUpdate();
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt);
      }
   }


   private final String selectTermMetaSQL;

   /**
    * Selects metadata for a term.
    * @param termId The term id.
    * @return The metadata.
    * @throws SQLException on database error.
    */
   public List<Meta> selectTermMeta(final long termId) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Meta> meta = Lists.newArrayListWithExpectedSize(8);
      Timer.Context ctx = metrics.selectTermMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTermMetaSQL);
         stmt.setLong(1, termId);
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

   final String insertTermMetaSQL;

   /**
    * Sets metadata for a term.
    * <p>
    *    Clears existing metadata.
    * </p>
    * @param termId The term id.
    * @param termMeta The metadata.
    * @throws SQLException on database error.
    */
   public void setTermMeta(final long termId, final List<Meta> termMeta) throws SQLException {

      clearTermMeta(termId);

      if(termMeta == null || termMeta.size() == 0) {
         return;
      }
      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.setTermMetaTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(insertTermMetaSQL);
         for(Meta meta : termMeta) {
            stmt.setLong(1, termId);
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
      Timer.Context ctx = metrics.createTermTimer.time();
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
      Timer.Context ctx = metrics.selectTermTimer.time();
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
      Timer.Context ctx = metrics.selectTaxonomyTermTimer.time();
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

   final String updateTaxonomyTermDescriptionSQL;

   /**
    * Sets the description for a taxonomy term.
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @param description The description.
    * @return Was the description set?
    * @throws SQLException on database error.
    */
   public boolean setTaxonomyTermDescription(final String taxonomy, final String name,
                                             final String description) throws SQLException {

      TaxonomyTerm term = selectTaxonomyTerm(taxonomy, name);
      if(term == null || term.term == null) {
         return false;
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.updateTaxonomyTermTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(updateTaxonomyTermDescriptionSQL);
         stmt.setString(1, Strings.nullToEmpty(description));
         stmt.setLong(2, term.term.id);
         stmt.setString(3, taxonomy);
         return stmt.executeUpdate() > 0;
      } finally {
         ctx.stop();
         closeQuietly(conn, stmt);
      }
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
      Timer.Context ctx = metrics.createTaxonomyTermTimer.time();
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
    * Resolves a taxonomy term.
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @return The taxonomy term or {@code null} if not found.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm resolveTaxonomyTerm(final String taxonomy, final String name) throws SQLException {
      TaxonomyTerm term;
      Cache<String, TaxonomyTerm> taxonomyTermCache = taxonomyTermCaches.get(taxonomy);
      if(taxonomyTermCache != null) {
         metrics.taxonomyTermCacheTries.mark();
         term = taxonomyTermCache.getIfPresent(name);
         if(term != null) {
            metrics.taxonomyTermCacheHits.mark();
            return term;
         }
      }

      term = selectTaxonomyTerm(taxonomy, name);
      if(term != null && taxonomyTermCache != null) {
         taxonomyTermCache.put(name, term);
      }

      return term;
   }

   /**
    * Resolves a taxonomy term, creating one if it does not exist.
    * <p>
    *    If taxonomy term cache is configured for this taxonomy, it
    *    is used for resolution.
    * </p>
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @param description The description to be used of the taxonmy term is created.
    * @return The taxonomy term.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm resolveTaxonomyTerm(final String taxonomy, final String name, final String description) throws SQLException {

      TaxonomyTerm term;
      Cache<String, TaxonomyTerm> taxonomyTermCache = taxonomyTermCaches.get(taxonomy);
      if(taxonomyTermCache != null) {
         metrics.taxonomyTermCacheTries.mark();
         term = taxonomyTermCache.getIfPresent(name);
         if(term != null) {
            metrics.taxonomyTermCacheHits.mark();
            return term;
         }
      }

      term = selectTaxonomyTerm(taxonomy, name);
      if(term == null) {
         term = createTaxonomyTerm(taxonomy, name, slugify(name), description);
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

      metrics.taxonomyTermCacheTries.mark();
      TaxonomyTerm term = taxonomyTermCache.getIfPresent(id);
      if(term != null) {
         metrics.taxonomyTermCacheHits.mark();
         return term;
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      long termId = 0;
      String taxonomy = "";
      String description = "";
      Timer.Context ctx = metrics.taxonomyTermResolveTimer.time();
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
      Timer.Context ctx = metrics.postTermsClearTimer.time();
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
      Timer.Context ctx = metrics.postTermsClearTimer.time();
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
    *    Uses cache, if configured, to resolve. If terms are created, they will
    *    have an empty description.
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
         taxonomyTerms.add(resolveTaxonomyTerm(taxonomy, term, ""));
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      Timer.Context ctx = metrics.postTermsSetTimer.time();
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
      Timer.Context ctx = metrics.postTermsSelectTimer.time();
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
      Timer.Context ctx = metrics.optionSelectTimer.time();
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

   private static final String selectBlogSQL = "SELECT blog_id, site_id, domain, path, registered, last_updated FROM wp_blogs";

   /**
    * Creates a blog from a result set.
    * @param rs The result set.
    * @return The blog.
    * @throws SQLException on database error.
    */
   private Blog blogFromResultSet(final ResultSet rs) throws SQLException {

      long registeredTimestamp = 0L;
      long lastUpdatedTimestamp = 0L;

      try {
         registeredTimestamp = rs.getTimestamp(5).getTime();
      } catch(SQLException se) {
         //Deal with possible java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
         registeredTimestamp = 0L;
      }

      try {
         lastUpdatedTimestamp = rs.getTimestamp(6).getTime();
      } catch(SQLException se) {
         //Deal with possible java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
         lastUpdatedTimestamp = 0L;
      }

      return new Blog(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), registeredTimestamp, lastUpdatedTimestamp);
   }

   private static final String selectPublicBlogsSQL = selectBlogSQL + " WHERE deleted=0 AND public=1";

   /**
    * Selects all public, enabled blogs.
    * @return The list of blogs.
    * @throws SQLException on database error.
    */
   public List<Blog> selectPublicBlogs() throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Blog> blogs = Lists.newArrayListWithExpectedSize(4);
      Timer.Context ctx = metrics.selectBlogsTimer.time();
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectPublicBlogsSQL);
         rs = stmt.executeQuery();
         while(rs.next()) {
            blogs.add(blogFromResultSet(rs));
         }
      } finally {
         ctx.stop();
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      return blogs;
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return metrics.getMetrics();
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
   private final Duration taxonomyCacheTimeout;
   private final Cache<Long, TaxonomyTerm> taxonomyTermCache;
   private final Metrics metrics;
   private static final Joiner inJoiner = Joiner.on(',').skipNulls();
}
