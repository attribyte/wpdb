# Attribyte WPDB

A Java library that provides read/write access to WordPress &trade; databases.

Features
--------

* Create, select, update and delete users.
* Optional user cache.
* Insert, update and delete posts.
* Posts may be inserted with a known `ID` or auto-generated.
* Select posts by id.
* Select posts by author.
* Select posts by "slug."
* Select children for posts.
* Select posts sorted by publish time, last modified (inefficient out-of-box), id.
* Select posts associated with one or more taxonomy terms.
* Page posts with start, limit and timestamp constraints.
* Set and delete post children, e.g. attachments.
* Set and delete post metadata.
* Set and delete terms, taxonomy terms and their post associations.
* Optional taxonomy terms cache.   
* Select options e.g. site configuration.
* Multiple site support.
* Fully instrumented with [Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/).
* JUnit tests.
 
Missing Features
----------------

* Comments
* Complete user metadata
* Blog/versions tables.


Requirements
------------

* [JRE/JDK 8+](http://www.oracle.com/technetwork/java/javase/downloads/index.html) 

Build
-----

Build requires [Ant](http://ant.apache.org/) and [Ivy](http://ant.apache.org/ivy/) for
resolving dependencies.