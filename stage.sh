#!/bin/sh
VERSION="1.0.0"
cp wpdb.pom dist/lib/wpdb-${VERSION}.pom
cd dist/lib
gpg -ab wpdb-${VERSION}.pom
gpg -ab wpdb-${VERSION}.jar
gpg -ab wpdb-${VERSION}-sources.jar
gpg -ab wpdb-${VERSION}-javadoc.jar
jar -cvf ../bundle.jar *

