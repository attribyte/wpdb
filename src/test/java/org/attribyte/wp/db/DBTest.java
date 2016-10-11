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
import org.attribyte.wp.model.Blog;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Paging;
import org.attribyte.wp.model.Post;
import org.attribyte.wp.model.TaxonomyTerm;
import org.attribyte.wp.model.Term;
import org.attribyte.wp.model.User;
import org.joda.time.Interval;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
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
         props.put("user", System.getProperty("dbuser", "root"));
         props.put("password", System.getProperty("dbpass", ""));
         props.put("driver", "com.mysql.jdbc.Driver");
         props.put("host", System.getProperty("dbhost", "localhost"));
         props.put("db", System.getProperty("db", "wordpress_test"));
         _db = new DB(new SimpleConnectionSource(props), 1, ImmutableSet.of("test_taxonomy_with_cache"),
                 Duration.ofMinutes(30), Duration.ofMinutes(30));
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
      Post testPost = createTestPost(createdUser, 1000).withId(0L);
      Post createdPost = db().insertPost(testPost, TimeZone.getDefault());
      assertNotNull(createdPost);
      assertTrue(createdPost.id > 0L);
   }

   @Test
   public void insertPostWithId() throws Exception {
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
   public void updatePost() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1000);
      Post testPost = createTestPost(createdUser, 1000);
      db().insertPost(testPost, TimeZone.getDefault());
      Post.Builder insertedPost = db().selectPost(testPost.id);
      assertNotNull(insertedPost);
      insertedPost.setTitle("T");
      insertedPost.setExcerpt("E");
      insertedPost.setContent("C");
      insertedPost.setGUID("G");
      Post updatedPost = db().updatePost(insertedPost.build(), TimeZone.getDefault());
      assertEquals("T", updatedPost.title);
      assertEquals("E", updatedPost.excerpt);
      assertEquals("C", updatedPost.content);
      assertEquals("G", updatedPost.guid);
   }

   @Test
   public void parentChild() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testParent = createTestPost(createdUser, -1);
      db().insertPost(testParent, TimeZone.getDefault());

      Post testChild = createTestPost(createdUser, -1);
      testChild = testChild.withParent(testParent.id);
      db().insertPost(testChild, TimeZone.getDefault());

      List<Post> children = db().selectChildren(testParent.id, false);
      assertNotNull(children);
      assertEquals(1, children.size());

      db().deleteChildren(testParent.id);
      children = db().selectChildren(testParent.id, false);
      assertNotNull(children);
      assertEquals(0, children.size());
   }

   @Test
   public void idSelectWithTerms() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testPost0 = createTestPost(createdUser, -1);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().setPostTerms(testPost0.id, "test_taxonomy", ImmutableList.of("test0", "test1"));

      List<Long> posts = db().selectPostIds(Post.Type.POST, Post.Status.PUBLISH, ImmutableList.of(),Post.Sort.DESC, new Paging(0, 2));
      assertNotNull(posts);
      assertTrue(posts.size() > 0);

      TaxonomyTerm test0 = db().resolveTaxonomyTerm("test_taxonomy", "test0");
      assertNotNull(test0);

      TaxonomyTerm test1 = db().resolveTaxonomyTerm("test_taxonomy", "test1");
      assertNotNull(test1);

      posts = db().selectPostIds(Post.Type.POST, Post.Status.PUBLISH, ImmutableList.of(test0),Post.Sort.DESC, new Paging(0, 2));
      assertNotNull(posts);
      assertTrue(posts.size() > 0);
      assertEquals((Long)testPost0.id, posts.get(0));

      posts = db().selectPostIds(Post.Type.POST, Post.Status.PUBLISH, ImmutableList.of(test0, test1),Post.Sort.DESC, new Paging(0, 2));
      assertNotNull(posts);
      assertTrue(posts.size() > 0);
      assertEquals((Long)testPost0.id, posts.get(0));
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
   public void postSlug() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testPost0 = createTestPost(createdUser, -1);
      db().insertPost(testPost0, TimeZone.getDefault());

      List<Post> posts = db().selectPosts(testPost0.slug, false);
      assertNotNull(posts);
      assertEquals(1, posts.size());
   }

   @Test
   public void postPaging() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testPost0 = createTestPost(createdUser, -1);
      Post testPost1 = createTestPost(createdUser, -1);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().insertPost(testPost1, TimeZone.getDefault());

      List<Post> posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.DESC, new Paging(0, 2), true);
      assertNotNull(posts);
      assertEquals(2, posts.size());

      posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.ID_DESC, new Paging(0, 2), true);
      assertNotNull(posts);
      assertEquals(2, posts.size());
      assertTrue(posts.get(0).id > posts.get(1).id);

      posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.ID_ASC, new Paging(0, 2), true);
      assertNotNull(posts);
      assertEquals(2, posts.size());
      assertTrue(posts.get(0).id < posts.get(1).id);
   }

   @Test
   public void modPosts() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1000L);
      db().deletePost(1001L);
      Post testPost0 = createTestPost(createdUser, 1000L);
      Post testPost1 = createTestPost(createdUser, 1001L);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().insertPost(testPost1, TimeZone.getDefault());
      List<Post> posts = db().selectModifiedPosts(null, 0L, 0L, 2, false);
      assertNotNull(posts);
      assertEquals(2, posts.size());

      posts = db().selectModifiedPosts(null, System.currentTimeMillis() + 3600L * 24L * 1000L, testPost1.id, 2, false);
      assertEquals(0, posts.size());
   }

   @Test
   public void modPostsWithType() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      db().deletePost(1000L);
      db().deletePost(1001L);
      Post testPost0 = createTestPost(createdUser, 1000L);
      Post testPost1 = createTestPost(createdUser, 1001L);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().insertPost(testPost1, TimeZone.getDefault());
      List<Post> posts = db().selectModifiedPosts(Post.Type.POST, 0L, 0L, 2, false);
      assertNotNull(posts);
      assertEquals(2, posts.size());
   }

   @Test
   public void postPagingInterval() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testPost0 = createTestPost(createdUser, -1);
      Post testPost1 = createTestPost(createdUser, -1);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().insertPost(testPost1, TimeZone.getDefault());

      Interval interval = new Interval(0, System.currentTimeMillis() + 10000L);
      List<Post> posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.DESC, new Paging(0, 2, interval), true);
      assertNotNull(posts);
      assertEquals(2, posts.size());

      posts = db().selectPosts(Post.Type.POST, Post.Status.PUBLISH, Post.Sort.DESC, new Paging(0, 1, interval), true);
      assertNotNull(posts);
      assertEquals(1, posts.size());
   }

   @Test
   public void authorPosts() throws Exception {
      String username = StringUtil.randomString(8);
      User user = new User(0L, username, username.toUpperCase(), username + "@testy.com", System.currentTimeMillis(), ImmutableList.of());
      User createdUser = db().createUser(user, "XXXX");
      Post testPost0 = createTestPost(createdUser, -1);
      Post testPost1 = createTestPost(createdUser, -1);
      db().insertPost(testPost0, TimeZone.getDefault());
      db().insertPost(testPost1, TimeZone.getDefault());
      List<Post> posts = db().selectAuthorPosts(createdUser.id, Post.Sort.DESC, new Paging(0,2), true);
      assertNotNull(posts);
      assertEquals(2, posts.size());

      posts = db().selectAuthorPosts(createdUser.id, Post.Sort.DESC, new Paging(0,1), true);
      assertNotNull(posts);
      assertEquals(1, posts.size());

      posts = db().selectAuthorPosts(createdUser.id, Post.Sort.DESC, new Paging(2,1), true);
      assertNotNull(posts);
      assertEquals(0, posts.size());
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

   @Test
   public void selectBlogs() throws Exception {
      List<Blog> blogs = db().selectPublicBlogs();
      assertNotNull(blogs);
      assertTrue(blogs.size() > 0);
   }

   /**
    * Creates a test post for a user.
    * @param user The user.
    * @param id The id. If < 0, random id is generated.
    * @return The test post.
    */
   private Post createTestPost(final User user, long id) {
      Post.Builder builder = Post.newBuilder();
      String rndStr = StringUtil.randomString(8);
      if(id < 0) {
         id = Math.abs(rnd.nextInt());
      }

      builder.setId(id);
      builder.setTitle("Test title " + rndStr);
      builder.setContent("Test content " + rndStr);
      builder.setAuthorId(user.id);
      builder.setExcerpt("Text excerpt " + rndStr);
      builder.setSlug(rndStr);
      builder.setGUID("http://localhost?id="+rndStr);
      builder.setPublishTimestamp(System.currentTimeMillis());
      builder.setModifiedTimestamp(System.currentTimeMillis());
      builder.setStatus(Post.Status.PUBLISH);
      builder.setType(Post.Type.POST);
      return builder.build();
   }

   private static final Random rnd = new Random();
}
