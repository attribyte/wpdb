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

import com.google.common.collect.ImmutableSet;
import org.attribyte.wp.model.Shortcode;
import org.junit.Test;

import java.text.ParseException;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests for shortcode parsing.
 */
public class ShortcodeTest {

   static class BufferHandler implements Shortcode.Handler {

      BufferHandler(final Set<String> expectContent) {
         this.expectContent = ImmutableSet.copyOf(expectContent);
      }

      BufferHandler() {
         this.expectContent = ImmutableSet.of();
      }


      public void shortcode(Shortcode shortcode) {
         buf.append(shortcode.toString());
      }

      public void text(String text) {
         buf.append(text);
      }

      public void parseError(String text, ParseException pe) {
         buf.append(text);
         lastErrorText = text;
         lastParseException = pe;
      }

      public boolean expectContent(final String shortcode) {
         return expectContent.contains(shortcode);
      }

      StringBuilder buf = new StringBuilder();
      String lastErrorText;
      ParseException lastParseException;
      ImmutableSet<String> expectContent;
   }

   @Test
   public void handleSimpleShortcode() throws Exception {
      String text = "[testcode testval]";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleInvalidStart() throws Exception {
      String text = "[testcode some text a=\"b\"";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void textOnly() throws Exception {
      String text = "this is some text ] ";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleExpectedContent() throws Exception {
      String text = "[testcode testval]the text[/testcode]";
      BufferHandler handler = new BufferHandler(ImmutableSet.of("testcode"));
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMismatchEnd() throws Exception {
      String text = "[testcode testval]the text[/testcodex]";
      BufferHandler handler = new BufferHandler(ImmutableSet.of("testcode"));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleInvalidEnd() throws Exception {
      String text = "[/testcode testval]the text[/testcodex]";
      BufferHandler handler = new BufferHandler(ImmutableSet.of("testcode"));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMixed() throws Exception {
      String text = "some text [testcode testval] some more text";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handlMulti() throws Exception {
      String text = "[testcode testval] [testcode2 a=\"b\"] end";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMixedMulti() throws Exception {
      String text = "some text [testcode testval] some more text [testcode2 a=\"b\"] end";
      BufferHandler handler = new BufferHandler();
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }
}

