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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.api.InitializationException;
import org.attribyte.util.StringUtil;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Paging;
import org.attribyte.wp.model.Post;
import org.attribyte.wp.model.TaxonomyTerm;
import org.attribyte.wp.model.Term;
import org.attribyte.wp.model.User;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.*;

public class DBTest {

   private static AtomicBoolean isInit = new AtomicBoolean(false);
   private static DB _db;

   protected DB db() throws InitializationException {
      if(isInit.compareAndSet(false, true)) {
         Properties props = new Properties();
         props.put("user", "root");
         props.put("password", "");
         props.put("driver", "com.mysql.jdbc.Driver");
         props.put("host", "localhost");
         props.put("db", "wordpress_test");
         _db = new DB(new SimpleConnectionSource(props), 1, ImmutableSet.of("test_taxonomy_with_cache"));
      }
      return _db;
   }


   @Test
   public void createUser() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      assertNotNull(createdUser);
      assertTrue(createdUser.id > 0L);
   }

   @Test
   public void userById() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      user = db().selectUser(createdUser.id);
      assertNotNull(user);
      assertEquals(createdUser.username, user.username);
   }

   @Test
   public void userByUsername() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      user = db().selectUser(createdUser.username);
      assertNotNull(user);
      assertEquals(createdUser.id, user.id);
   }

   @Test
   public void insertPost() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1000);
      Post testPost = createTestPost(createdUser, 1000);
      db().insertPost(testPost, TimeZone.getDefault());

      Post.Builder insertedPost = db().selectPost(testPost.id);
      assertNotNull(insertedPost);
   }

   @Test
   public void resolvePost() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1001);
      Post testPost = createTestPost(createdUser, 1001);
      db().insertPost(testPost, TimeZone.getDefault());
      List<Meta> metaList = Lists.newArrayList();
      metaList.add(new Meta(0L, "test0", "mval0"));
      metaList.add(new Meta(0L, "test1", "mval1"));
      db().setPostMeta(1001, metaList);
      db().setPostTerms(1001, "test_taxonomy", ImmutableList.of("term0", "term1"));

      Post.Builder builder = db().selectPost(1001);
      assertNotNull(builder);
      Post post = db().resolve(builder).build();

      assertNotNull(post.author);
      assertEquals(createdUser.id, post.author.id);

      assertNotNull(post.metadata);
      assertEquals(2, post.metadata.size());

      assertNotNull(post.taxonomyTerms);
      assertEquals(1,post.taxonomyTerms.size());

      List<TaxonomyTerm> terms = post.taxonomyTerms.get("test_taxonomy");
      assertNotNull(terms);
      assertEquals(2, terms.size());
   }

   @Test
   public void postPaging() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1001);
      Post testPost = createTestPost(createdUser, 1001);
      db().insertPost(testPost, TimeZone.getDefault());

      List<Post> posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.DESC, new Paging(0, 2), true);
      assertNotNull(posts);
      assertTrue(posts.size() > 0);
   }

   @Test
   public void postPagingInterval() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1001);
      Post testPost = createTestPost(createdUser, 1001);
      db().insertPost(testPost, TimeZone.getDefault());
      Interval interval = new Interval(0, System.currentTimeMillis() + 1000L);
      List<Post> posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.DESC, new Paging(0, 2, interval), true);
      assertNotNull(posts);
      assertTrue(posts.size() > 0);
   }

   @Test
   public void createTerm() throws Exception {
      String termName = StringUtil.randomString(8);
      Term term0 = db().createTerm(termName, "slug0-" + termName);
      assertNotNull(term0);
      assertTrue(term0.id > 0);

      Term term1 = db().createTerm(termName, "slug1-" + termName);
      assertNotNull(term1);
      assertTrue(term1.id > 0);
      assertNotEquals(term1.slug, term0.slug);

      Set<Long> newIds = ImmutableSet.of(term0.id, term1.id);
      Set<Long> checkIds = db().selectTermIds(termName);
      assertEquals(0, Sets.difference(newIds, checkIds).size());
   }

   @Test
   public void createTaxonomyTerm() throws Exception {
      String termName = StringUtil.randomString(8);
      TaxonomyTerm term0 = db().createTaxonomyTerm("test_taxonomy", termName, termName+"-slug0", termName+"-description0");
      assertNotNull(term0);
      assertTrue(term0.id > 0);
      assertNotNull(term0.term);
      assertNotNull(db().selectTerm(term0.id));

      TaxonomyTerm matchTerm = db().selectTaxonomyTerm("test_taxonomy", termName);
      assertNotNull(matchTerm);
      assertEquals(term0.id, matchTerm.id);
   }

   @Test
   public void resolveTaxonomyTerm() throws Exception {
      String termName = StringUtil.randomString(8);
      TaxonomyTerm term0 = db().resolveTaxonomyTerm("test_taxonomy", termName);
      assertNotNull(term0);
      assertTrue(term0.id > 0);
      assertNotNull(term0.term);
      assertNotNull(db().selectTerm(term0.id));

      TaxonomyTerm matchTerm = db().selectTaxonomyTerm("test_taxonomy", termName);
      assertNotNull(matchTerm);
      assertEquals(term0.id, matchTerm.id);
   }

   @Test
   public void resolveTaxonomyTermWithCache() throws Exception {
      String termName = StringUtil.randomString(8);
      TaxonomyTerm term0 = db().resolveTaxonomyTerm("test_taxonomy_with_cache", termName);
      assertNotNull(term0);
      assertTrue(term0.id > 0);
      assertNotNull(term0.term);
      assertNotNull(db().selectTerm(term0.id));

      TaxonomyTerm matchTerm = db().resolveTaxonomyTerm("test_taxonomy_with_cache", termName);
      assertNotNull(matchTerm);
      assertEquals(term0.id, matchTerm.id);
   }

   private Post createTestPost(final User user, final long id) {
      Post.Builder builder = Post.newBuilder();
      String rnd = StringUtil.randomString(8);
      builder.setId(id);
      builder.setTitle("Test title " + rnd);
      builder.setContent("Test content " + rnd);
      builder.setAuthorId(user.id);
      builder.setExcerpt("Text excerpt " + rnd);
      builder.setSlug(rnd);
      builder.setGUID("http://localhost?id="+rnd);
      builder.setPublishTimestamp(System.currentTimeMillis());
      builder.setModifiedTimestamp(System.currentTimeMillis());
      builder.setStatus(Post.Status.PUBLISH);
      builder.setType(Post.Type.POST);
      return builder.build();
   }

   @Test
   public void postTerms() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1002);
      Post testPost = createTestPost(createdUser, 1002);
      db().insertPost(testPost, TimeZone.getDefault());

      db().setPostTerms(1002, "test_taxonomy", ImmutableList.of("tag1", "tag2"));
      List<TaxonomyTerm> terms = db().selectPostTerms(1002, "test_taxonomy");
      assertNotNull(terms);
      assertEquals(2, terms.size());

      db().clearPostTerms(1002);
      terms = db().selectPostTerms(1002, "test_taxonomy");
      assertNotNull(terms);
      assertEquals(0, terms.size());
   }

   @Test
   public void postTermsCache() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1003);
      Post testPost = createTestPost(createdUser, 1003);
      db().insertPost(testPost, TimeZone.getDefault());

      db().setPostTerms(1003, "test_taxonomy", ImmutableList.of("tag1", "tag2"));
      db().setPostTerms(1003, "test_taxonomy_with_cache", ImmutableList.of("tag1", "tag4", "tag5"));

      List<TaxonomyTerm> terms = db().selectPostTerms(1003, "test_taxonomy");
      assertNotNull(terms);
      assertEquals(2, terms.size());

      terms = db().selectPostTerms(1003, "test_taxonomy_with_cache");
      assertNotNull(terms);
      assertEquals(3, terms.size());

      terms = db().selectPostTerms(1003);
      assertNotNull(terms);
      assertEquals(5, terms.size());

      db().clearPostTerms(1003, "test_taxonomy_with_cache");

      terms = db().selectPostTerms(1003, "test_taxonomy"); //Verify all are not deleted with set.
      assertNotNull(terms);
      assertEquals(2, terms.size());

   }

}