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


import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.sql.ConnectionSupplier;
import org.attribyte.util.SQLUtil;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Post;
import org.attribyte.wp.model.TaxonomyTerm;
import org.attribyte.wp.model.Term;
import org.attribyte.wp.model.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.attribyte.util.SQLUtil.closeQuietly;

public class DB {

   /**
    * Creates a database with a connection supplier.
    * @param connectionSupplier The connection supplier.
    * @param siteId The site id.
    * @param cachedTaxonomies A set of cached taxonomies.
    */
   public DB(final ConnectionSupplier connectionSupplier,
             final long siteId,
             final Set<String> cachedTaxonomies) {
      this.connectionSupplier = connectionSupplier;

      if(siteId < 2) {
         this.postsTableName = "wp_posts";
         this.postMetaTableName = "wp_postmeta";
         this.optionsTableName = "wp_options";
         this.termsTableName = "wp_terms";
         this.termRelationshipsTableName = "wp_term_relationships";
         this.termTaxonomyTableName = "wp_term_taxonomy";
      } else {
         this.postsTableName = "wp_" + siteId + "_posts";
         this.postMetaTableName = "wp_" + siteId + "_postmeta";
         this.optionsTableName = "wp_" + siteId + "_options";
         this.termsTableName = "wp_" + siteId + "_terms";
         this.termRelationshipsTableName = "wp_" + siteId + "_term_relationships";
         this.termTaxonomyTableName = "wp_" + siteId + "_term_taxonomy";
      }

      this.userCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(30, TimeUnit.MINUTES) //Configure..
              .build();

      this.usernameCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(30, TimeUnit.MINUTES) //Configure..
              .build();

      ImmutableMap.Builder<String, Cache<String, TaxonomyTerm>> taxonomyTermCachesBuilder = ImmutableMap.builder();
      for(String taxonomy : cachedTaxonomies) {
         taxonomyTermCachesBuilder.put(taxonomy,
                 CacheBuilder.newBuilder()
                 .concurrencyLevel(4)
                 .expireAfterWrite(30, TimeUnit.MINUTES)
                 .build()
                 );
      }
      this.taxonomyTermCaches = taxonomyTermCachesBuilder.build();

      this.selectTermIdSQL = "SELECT name, slug FROM " + termsTableName + " WHERE term_id=?";

      this.selectTaxonomyTermSQL = "SELECT term_taxonomy_id," + termTaxonomyTableName + ".term_id, description " +
              "FROM " + termsTableName + "," + termTaxonomyTableName + " WHERE " + termsTableName + ".name=? " +
              "AND taxonomy=? AND " +termsTableName + ".term_id=" + termTaxonomyTableName + ".term_id";
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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(createUserSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setString(1, user.username);
         stmt.setString(2, Strings.nullToEmpty(userPass));
         stmt.setString(3, user.displayName());
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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserByUsernameSQL);
         stmt.setString(1, username);
         rs = stmt.executeQuery();
         return rs.next() ? userFromResultSet(rs) : null;
      } finally {
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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectUserByIdSQL);
         stmt.setLong(1, userId);
         rs = stmt.executeQuery();
         return rs.next() ? userFromResultSet(rs) : null;
      } finally {
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
      User user = userCache.getIfPresent(userId);
      if(user != null) {
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
      User user = usernameCache.getIfPresent(username);
      if(user != null) {
         return user;
      } else {
         user = selectUser(username);
         if(user != null) {
            usernameCache.put(username, user);
         }
         return user;
      }
   }

   private static final String selectUserMetaSQL = "SELECT umeta_id, meta_key, meta_value WHERE userId=?";

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
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Deletes a post with a specified id, including all associated metadata.
    * @param postId The post id.
    * @throws SQLException on database error.
    */
   public void deletePost(final long postId) throws SQLException {
      clearPostMeta(postId);
      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("DELETE FROM " + postsTableName + " WHERE ID=?");
         stmt.setLong(1, postId);
         stmt.executeUpdate();
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private static final String insertPostWithIdSQL =
           " (ID, post_author, post_date, post_date_gmt, post_content, post_title, " +
                   "post_excerpt, post_status, post_name, post_modified, post_modified_gmt," +
                   "post_parent, guid, post_type, to_ping, pinged, post_content_filtered) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, '','', '')";


   /**
    * Inserts a post.
    * @param post The post.
    * @param tz The local time zone for the post.
    * @throws SQLException on database error.
    */
   public void insertPost(final Post post, final TimeZone tz) throws SQLException {
      int offset = tz.getOffset(post.publishTimestamp);
      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("INSERT INTO " + postsTableName + insertPostWithIdSQL);
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
         stmt.setLong(12, 0);
         stmt.setString(13, Strings.nullToEmpty(post.guid));
         stmt.setString(14, "post");
         stmt.executeUpdate();
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   /**
    * Clears all metadata for a post.
    * @param postId The post id.
    * @throws SQLException on database error.
    */
   public void clearPostMeta(final long postId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("DELETE FROM " + postMetaTableName + " WHERE post_id=?");
         stmt.setLong(1, postId);
         stmt.executeUpdate();
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("INSERT INTO " + postMetaTableName + "(post_id, meta_key, meta_value) VALUES (?,?,?)");
         for(Meta meta : postMeta) {
            stmt.setLong(1, postId);
            stmt.setString(2, meta.key);
            stmt.setString(3, meta.value);
            stmt.executeUpdate();
         }
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

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
         stmt = conn.prepareStatement("SELECT term_id FROM " + termsTableName + " WHERE name=?");
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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("INSERT INTO " + termsTableName + "(name, slug) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement(selectTermIdSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         return rs.next() ? new Term(id, rs.getString(1), rs.getString(2)) : null;
      } finally {
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
         closeQuietly(conn, stmt, rs);
      }
      return new TaxonomyTerm(taxonomyTermId, selectTerm(termId), description);
   }

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
      try {
         conn = connectionSupplier.getConnection();
         stmt = conn.prepareStatement("INSERT INTO " + termTaxonomyTableName + "(term_id, taxonomy, description) VALUES (?,?, ?)", Statement.RETURN_GENERATED_KEYS);
         stmt.setLong(1, term.id);
         stmt.setString(2, taxonomy);
         stmt.setString(3, Strings.nullToEmpty(description));
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return new TaxonomyTerm(rs.getLong(1), term, description);
         } else {
            throw new SQLException("Problem creating taxonomy term (no generated id)");
         }
      } finally {
         closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Resolves a taxonomy term, creating if required.
    * @param taxonomy The taxonomy.
    * @param name The term name.
    * @return The taxonomy term.
    * @throws SQLException on database error.
    */
   public TaxonomyTerm resolveTaxonomyTerm(final String taxonomy, final String name) throws SQLException {

      TaxonomyTerm term;
      Cache<String, TaxonomyTerm> taxonomyTermCache = taxonomyTermCaches.get(taxonomy);
      if(taxonomyTermCache != null) {
         term = taxonomyTermCache.getIfPresent(name);
         if(term != null) {
            return term;
         }
      }

      term = selectTaxonomyTerm(taxonomy, name);
      if(term != null) {
         if(taxonomyTermCache != null) {
            taxonomyTermCache.put(name, term);
         }
         return term;
      } else {
         return null; //TODO!!!
      }
   }

   private final ConnectionSupplier connectionSupplier;

   private final String optionsTableName;
   private final String postsTableName;
   private final String postMetaTableName;
   private final String termsTableName;
   private final String termRelationshipsTableName;
   private final String termTaxonomyTableName;

   private final Cache<Long, User> userCache;
   private final Cache<String, User> usernameCache;
   private final ImmutableMap<String, Cache<String, TaxonomyTerm>> taxonomyTermCaches;
}
