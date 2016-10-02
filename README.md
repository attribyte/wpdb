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
* Select modified posts without skip due to long sequences with identical modified times.
* Page posts with start, limit and timestamp constraints.
* Set and delete post children, e.g. attachments.
* Set and delete post metadata.
* Set and delete terms, taxonomy terms and their post associations.
* Optional taxonomy terms cache.   
* Select options e.g. site configuration.
* Select public blogs.
* Multiple site support.
* Fully instrumented with [Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/).
* JUnit tests.
 
Missing Features
----------------

* Comments
* User metadata

Requirements
------------

* [JRE/JDK 8+](http://www.oracle.com/technetwork/java/javase/downloads/index.html) 

Build
-----

Build with [Ant](http://ant.apache.org/) and [Ivy](http://ant.apache.org/ivy/)

* Build: `ant dist`
* Build docs: `ant javadoc`
* Build and retrieve all dependencies: `ant full-dist`
* Build and run tests: `ant -Ddb=[database] -Ddbuser=[user] -Ddbpass=[password] -Ddbhost=[host] test` *Do Not Run Against a Production Database*

License
-------

&copy; 2016, [Attribyte, LLC](https://attribyte.com)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.