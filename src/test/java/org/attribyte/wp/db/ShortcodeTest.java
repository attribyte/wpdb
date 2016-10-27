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

import com.google.common.collect.ImmutableMap;
import org.attribyte.wp.model.Shortcode;
import org.attribyte.wp.model.ShortcodeParser;
import org.junit.Test;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for shortcode parsing functions.
 */
public class ShortcodeTest {

   static class BufferHandler implements ShortcodeParser.Handler {

      BufferHandler(final Map<String, Shortcode.Type> expectContent) {
         this.shortcodeTypes = ImmutableMap.copyOf(expectContent);
      }

      public void shortcode(Shortcode shortcode) {
         this.lastShortcode = shortcode;
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

      public Shortcode.Type type(final String shortcode) {
         Shortcode.Type type = shortcodeTypes.get(shortcode);
         return type != null ? type : Shortcode.Type.UNKNOWN;
      }

      StringBuilder buf = new StringBuilder();
      String lastErrorText;
      Shortcode lastShortcode = null;
      ParseException lastParseException;
      ImmutableMap<String, Shortcode.Type> shortcodeTypes;
   }

   @Test
   public void lowercased() throws Exception {
      String text = "[testcode TestVal=\"test'test\"]";
      Shortcode code = Shortcode.parse(text);
      assertNotNull(code.attributes.get("testval"));
      assertEquals("test\'test", code.attributes.get("testval"));
   }

   @Test
   public void doubleSingleQuote() throws Exception {
      String text = "[testcode testval=\"test'test\"]";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
   }

   @Test
   public void singleDoubleQuote() throws Exception {
      String text = "[testcode testval=\'test\"test\']";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
   }

   @Test
   public void noSpecial() throws Exception {
      String text = "[testcode testval=test2]";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
   }

   @Test
   public void positionalValue() throws Exception {
      String text = "[testcode testval]";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
      assertNotNull(code.positionalValue(0));
      assertEquals("testval", code.positionalValue(0));
   }

   @Test
   public void noValue() throws Exception {
      String text = "[testcode]";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
   }

   @Test
   public void positionalValues() throws Exception {
      String text = "[testcode testval x=y z=1 testval2]";
      Shortcode code = Shortcode.parse(text);
      List<String> values = code.positionalValues();
      assertNotNull(values);
      assertEquals(2, values.size());
      assertEquals("testval", values.get(0));
      assertEquals("testval2", values.get(1));
   }

   @Test
   public void positionalValueWithSpecial() throws Exception {
      String text = "[testcode \"test val\"]";
      Shortcode code = Shortcode.parse(text);
      assertEquals(text, code.toString());
   }

   @Test
   public void handleSimpleShortcode() throws Exception {
      String text = "[testcode testval]";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastShortcode);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void unknownTag() throws Exception {
      String text = "[testcodex testval]";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNull(handler.lastShortcode);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleInvalidStart() throws Exception {
      String text = "[testcode some text a=\"b\"";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNull(handler.lastShortcode);
      assertNotNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void textOnly() throws Exception {
      String text = "this is some text ] ";
      BufferHandler handler = new BufferHandler(ImmutableMap.of());
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleExpectedContent() throws Exception {
      String text = "[testcode testval]the text[/testcode]";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.ENCLOSING));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastShortcode);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMismatchEnd() throws Exception {
      String text = "[testcode testval]the text[/testcodex]";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.ENCLOSING));
      Shortcode.parse(text, handler);
      assertNull(handler.lastShortcode);
      assertNotNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleInvalidEnd() throws Exception {
      String text = "[/testcode testval]the text[/testcodex]";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNull(handler.lastShortcode);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMixed() throws Exception {
      String text = "some text [testcode testval] some more text";
      BufferHandler handler = new BufferHandler(ImmutableMap.of("testcode", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastShortcode);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handlMulti() throws Exception {
      String text = "[testcode testval] [testcode2 a=b] end";
      BufferHandler handler = new BufferHandler(ImmutableMap.of(
              "testcode", Shortcode.Type.SELF_CLOSING,
              "testcode2", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNull(handler.lastErrorText);
      assertNotNull(handler.lastShortcode);
      assertEquals("testcode2", handler.lastShortcode.name);
      assertEquals(text, handler.buf.toString());
   }

   @Test
   public void handleMixedMulti() throws Exception {
      String text = "some text [testcode testval] some more text [testcode2 a=b] end";
      BufferHandler handler = new BufferHandler(ImmutableMap.of(
              "testcode", Shortcode.Type.SELF_CLOSING,
              "testcode2", Shortcode.Type.SELF_CLOSING));
      Shortcode.parse(text, handler);
      assertNotNull(handler.lastShortcode);
      assertEquals("testcode2", handler.lastShortcode.name);
      assertNull(handler.lastErrorText);
      assertEquals(text, handler.buf.toString());
   }
}

