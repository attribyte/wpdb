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

import org.attribyte.api.InitializationException;
import org.attribyte.sql.ConnectionSupplier;
import org.attribyte.util.InitUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Creates a new connection on every invocation. Not for production use.
 */
public class SimpleConnectionSource implements ConnectionSupplier {

   /**
    * Creates a simple conneciton source.
    * @param props The configuration properties.
    * @throws InitializationException on initialization error.
    */
   public SimpleConnectionSource(final Properties props) throws InitializationException {
      init(props);
   }

   String host;
   String db;
   String user;
   String password;
   String port;
   String driver;
   String connectionString;

   private void init(final Properties props) throws InitializationException {

      InitUtil initProps = new InitUtil("", props);

      this.user = initProps.getProperty("user", "");
      this.password = initProps.getProperty("password", "");
      this.driver = initProps.getProperty("driver", "com.mysql.jdbc.Driver");

      try {
         Class.forName(this.driver);
      } catch(Exception e) {
         throw new InitializationException("Unable to initialize JDBC driver", e);
      }

      if(initProps.getProperty("connectionString") == null) {
         this.host = initProps.getProperty("host", null);
         this.port = initProps.getProperty("port", "3306");
         this.db = initProps.getProperty("db", null);

         if(host == null) {
            initProps.throwRequiredException("host");
         }

         if(db == null) {
            initProps.throwRequiredException("db");
         }

         this.connectionString = "jdbc:mysql://" + host + ":" + port + "/" + db;
      } else {
         this.connectionString = initProps.getProperty("connectionString");
      }
   }

   public Connection getConnection() throws SQLException {
      if(user != null && password != null) {
         return DriverManager.getConnection(connectionString, user, password);
      } else {
         return DriverManager.getConnection(connectionString);
      }
   }
}