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
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Timers, meters, etc.
 */
public class Metrics implements MetricSet {

   /**
    * Creates metrics.
    * @param metricSource The metric source.
    */
   Metrics(final MetricSource metricSource) {
      this.optionSelectTimer = metricSource.newTimer();
      this.postTermsSelectTimer = metricSource.newTimer();
      this.postTermsSetTimer = metricSource.newTimer();
      this.postTermsClearTimer = metricSource.newTimer();
      this.taxonomyTermResolveTimer = metricSource.newTimer();
      this.selectTaxonomyTermTimer = metricSource.newTimer();
      this.createTaxonomyTermTimer = metricSource.newTimer();
      this.createUserTimer = metricSource.newTimer();
      this.selectUserTimer = metricSource.newTimer();
      this.userMetadataTimer = metricSource.newTimer();
      this.clearUserMetaTimer = metricSource.newTimer();
      this.deletePostTimer = metricSource.newTimer();
      this.selectAuthorPostsTimer = metricSource.newTimer();
      this.selectPostsTimer = metricSource.newTimer();
      this.selectModPostsTimer = metricSource.newTimer();
      this.selectPostIdsTimer = metricSource.newTimer();
      this.selectChildrenTimer = metricSource.newTimer();
      this.deleteChildrenTimer = metricSource.newTimer();
      this.selectSlugPostsTimer = metricSource.newTimer();
      this.selectPostTimer = metricSource.newTimer();
      this.selectPostMapTimer = metricSource.newTimer();
      this.insertPostTimer = metricSource.newTimer();
      this.updatePostTimer = metricSource.newTimer();
      this.clearPostMetaTimer = metricSource.newTimer();
      this.selectPostMetaTimer = metricSource.newTimer();
      this.setPostMetaTimer = metricSource.newTimer();
      this.createTermTimer = metricSource.newTimer();
      this.selectTermTimer = metricSource.newTimer();
      this.resolvePostTimer = metricSource.newTimer();
      this.selectBlogsTimer = metricSource.newTimer();
      this.userCacheHits = metricSource.newMeter();
      this.userCacheTries = metricSource.newMeter();
      this.usernameCacheHits = metricSource.newMeter();
      this.usernameCacheTries = metricSource.newMeter();
      this.taxonomyTermCacheHits = metricSource.newMeter();
      this.taxonomyTermCacheTries = metricSource.newMeter();
   }

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
              .put("select-mod-posts", selectModPostsTimer)
              .put("select-post-ids", selectPostIdsTimer)
              .put("select-post-children", selectChildrenTimer)
              .put("delete-post_children", deleteChildrenTimer)
              .put("select-slug-post", selectSlugPostsTimer)
              .put("select-post", selectPostTimer)
              .put("select-post-map", selectPostMapTimer)
              .put("insert-post", insertPostTimer)
              .put("update-post", updatePostTimer)
              .put("resolve-post", resolvePostTimer)
              .put("select-blogs", selectBlogsTimer)
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

   Timer optionSelectTimer;
   Timer postTermsSelectTimer;
   Timer postTermsSetTimer;
   Timer postTermsClearTimer;
   Timer taxonomyTermResolveTimer;
   Timer selectTaxonomyTermTimer;
   Timer createTaxonomyTermTimer;
   Timer createUserTimer;
   Timer selectUserTimer;
   Timer userMetadataTimer;
   Timer clearUserMetaTimer;
   Timer deletePostTimer;
   Timer selectAuthorPostsTimer;
   Timer selectPostsTimer;
   Timer selectModPostsTimer;
   Timer selectPostIdsTimer;
   Timer selectChildrenTimer;
   Timer deleteChildrenTimer;
   Timer selectSlugPostsTimer;
   Timer selectPostTimer;
   Timer selectPostMapTimer;
   Timer insertPostTimer;
   Timer updatePostTimer;
   Timer clearPostMetaTimer;
   Timer selectPostMetaTimer;
   Timer setPostMetaTimer;
   Timer createTermTimer;
   Timer selectTermTimer;
   Timer resolvePostTimer;
   Timer selectBlogsTimer;
   Meter userCacheHits;
   Meter userCacheTries;
   Meter usernameCacheHits;
   Meter usernameCacheTries;
   Meter taxonomyTermCacheHits;
   Meter taxonomyTermCacheTries;
}
