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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Creates a new connection on every invocation. Not for production use.
 */
public class SimpleConnectionSource implements ConnectionSupplier {

   /**
    * Creates a simple connection source.
    * @param props The configuration properties.
    * @throws InitializationException on initialization error.
    */
   public SimpleConnectionSource(final Properties props) throws InitializationException {
      this.user = props.getProperty("user", "");
      this.password = props.getProperty("password", "");
      String driver = props.getProperty("driver", "com.mysql.jdbc.Driver");

      try {
         Class.forName(driver);
      } catch(Exception e) {
         throw new InitializationException(String.format("Unable to initialize JDBC driver '%s'", driver), e);
      }

      final String host;

      if(props.getProperty("connectionString") == null) {
         host = props.getProperty("host", null);
         String port = props.getProperty("port", "3306");
         String db = props.getProperty("db", null);

         if(host == null) {
            throw new InitializationException("A 'host' property must be specified");
         }

         if(db == null) {
            throw new InitializationException("A 'db' property must be specified");
         }
         this.connectionString = "jdbc:mysql://" + host + ":" + port + "/" + db;
      } else {
         this.connectionString = props.getProperty("connectionString");
      }
   }

   @Override
   public Connection getConnection() throws SQLException {
      return user != null && password != null ?
              DriverManager.getConnection(connectionString, user, password) :
              DriverManager.getConnection(connectionString);
   }

   /**
    * The connection string.
    */
   private final String connectionString;

   /**
    * The connection username.
    */
   private final String user;

   /**
    * The connection password.
    */
   private final String password;
}