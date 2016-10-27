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

package org.attribyte.wp.model;

import com.google.common.collect.ImmutableMap;

import java.text.ParseException;
import java.util.Map;

public class ShortcodeParser {

   /**
    * A handler for shortcode parse events.
    */
   public static interface Handler {

      /**
       * Parsed a shortcode.
       * @param shortcode The shortcode.
       */
      public void shortcode(Shortcode shortcode);

      /**
       * A block of text between shortcodes.
       * @param text The text.
       */
      public void text(String text);

      /**
       * A shortcode parse error.
       * @param text The text that could not be parsed as a shortcode.
       * @param pe A parse exception, or {@code null} if none.
       */
      public void parseError(String text, ParseException pe);

      /**
       * Identify the shortcode type.
       * @param shortcode The shortcode name.
       * @return The type or {@code UNKNOWN} if invalid/unknown code.
       */
      public Shortcode.Type type(final String shortcode);
   }

   private static boolean isNameCharacter(final char ch) {
      return (Character.isLetterOrDigit(ch) || ch == '_' || ch == '-');
   }

   private static String validateName(final String str) throws ParseException {
      for(char ch : str.toCharArray()) {
         if(!isNameCharacter(ch)) {
            throw new ParseException(String.format("Invalid name ('%s')", str), 0);
         }
      }
      return str;
   }

   /**
    * Parse '[shortcode attr0="val0" attr1="val1"]
    * @param str The shortcode string.
    * @return The shortcode.
    * @throws ParseException on invalid shortcode.
    */
   static Shortcode parseStart(final String str) throws ParseException {
      String exp = str.trim();

      if(exp.length() < 3) {
         throw new ParseException(String.format("Invalid shortcode ('%s')", str), 0);
      }

      if(exp.charAt(0) != '[') {
         throw new ParseException("Expecting '['", 0);
      }

      if(exp.charAt(exp.length() - 1) != ']') {
         throw new ParseException("Expecting ']'", exp.length() - 1);
      }

      exp = exp.substring(1, exp.length() - 1).trim();

      if(exp.length() == 0) {
         throw new ParseException(String.format("Invalid shortcode ('%s')", str), 0);
      }

      int attrStart = exp.indexOf(' ');
      if(attrStart < 0) {
         return new Shortcode(validateName(exp), ImmutableMap.of());
      } else {
         return new Shortcode(validateName(exp.substring(0, attrStart)), parseAttributes(exp.substring(attrStart).trim()));
      }
   }

   /**
    * Holds state for parsing attributes.
    */
   private static class AttributeString {

      AttributeString(final String str) {
         this.chars = str.toCharArray();
         this.buf = new StringBuilder();
      }

      /**
       * The current state.
       */
      enum StringState {

         /**
          * Before any recognized start character.
          */
         BEFORE_START,

         /**
          * Inside a single-quoted value.
          */
         SINGLE_QUOTED_VALUE,

         /**
          * Inside a double-quoted value.
          */
         DOUBLE_QUOTED_VALUE,

         /**
          * A value.
          */
         VALUE,
      }

      private String value() {
         String val = buf.toString();
         buf.setLength(0);
         if(ch == ' ') { //Eat trailing spaces...
            int currPos = pos;
            while(currPos < chars.length) {
               char currChar = chars[currPos];
               if(currChar != ' ') {
                  pos = currPos;
                  ch = chars[pos];
                  break;
               } else {
                  currPos++;
               }
            }
         }

         return val;
      }

      String nextString() throws ParseException {

         AttributeString.StringState state = AttributeString.StringState.BEFORE_START;

         while(pos < chars.length) {
            ch = chars[pos++];
            switch(ch) {
               case '=':
                  switch(state) {
                     case BEFORE_START:
                        state = AttributeString.StringState.VALUE;
                        break;
                     case SINGLE_QUOTED_VALUE:
                     case DOUBLE_QUOTED_VALUE:
                        buf.append(ch);
                        break;
                     case VALUE:
                        return value();
                  }
                  break;
               case ' ':
                  switch(state) {
                     case BEFORE_START:
                        break;
                     case SINGLE_QUOTED_VALUE:
                     case DOUBLE_QUOTED_VALUE:
                        buf.append(ch);
                        break;
                     case VALUE:
                        return value();
                  }
                  break;
               case '\"':
                  switch(state) {
                     case BEFORE_START:
                        state = AttributeString.StringState.DOUBLE_QUOTED_VALUE;
                        break;
                     case SINGLE_QUOTED_VALUE:
                        buf.append(ch);
                        break;
                     case DOUBLE_QUOTED_VALUE:
                        return value();
                     case VALUE:
                        throw new ParseException("Unexpected '\"'", pos);
                  }
                  break;
               case '\'':
                  switch(state) {
                     case BEFORE_START:
                        state = AttributeString.StringState.SINGLE_QUOTED_VALUE;
                        break;
                     case DOUBLE_QUOTED_VALUE:
                        buf.append(ch);
                        break;
                     case SINGLE_QUOTED_VALUE:
                        return value();
                     case VALUE:
                        throw new ParseException("Unexpected '\'", pos);
                  }
                  break;
               default:
                  switch(state) {
                     case BEFORE_START:
                        state = AttributeString.StringState.VALUE;
                        break;
                  }
                  buf.append(ch);
                  break;
            }
         }

         switch(state) {
            case VALUE:
               return buf.toString();
            case SINGLE_QUOTED_VALUE:
               throw new ParseException("Expected \'", pos);
            case DOUBLE_QUOTED_VALUE:
               throw new ParseException("Expected \"", pos);
            default:
               return null;
         }
      }

      char ch;
      int pos = 0;
      String last;

      final char[] chars;
      final StringBuilder buf;
   }

   /**
    * Parse attributes in a shortcode.
    * @param attrString The attribute string.
    * @return The map of attributes. Keys are <em>lower-case</em>.
    * @throws ParseException on invalid shortcode.
    */
   private static Map<String, String> parseAttributes(String attrString) throws ParseException {

      AttributeString str = new AttributeString(attrString);
      ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder(); //Immutable map preserves entry order.
      AttrState state = AttrState.NAME;
      String currName = "";
      String currString = "";
      int currPos = 0;
      while((currString = str.nextString()) != null) {
         switch(state) {
            case NAME:
               if(str.ch == '=') {
                  currName = currString;
                  state = AttrState.VALUE;
               } else {
                  attributes.put(String.format("$%d", currPos++), currString);
               }
               break;
            case VALUE:
               attributes.put(currName.toLowerCase(), currString);
               state = AttrState.NAME;
               break;
         }
      }
      return attributes.build();
   }

   /**
    * Attribute parse state.
    */
   private enum AttrState {
      /**
       * Parsing a name.
       */
      NAME,

      /**
       * Expecting a value.
       */
      VALUE;
   }

   /**
    * State for parsing with a handler.
    */
   private enum HandlerParseState {
      TEXT,
      SELF_CLOSING,
      START_ENCLOSING,
      CONTENT,
      START_END,
      END_NAME;
   }

   /**
    * Scans for a valid shortcode name.
    * @param pos The starting position.
    * @param chars The character array to scan.
    * @return The shortcode name or an empty string if none found or invalid character is encountered.
    */
   private static String scanForName(int pos, final char[] chars) {
      StringBuilder buf = new StringBuilder();
      while(pos < chars.length) {
         char ch = chars[pos++];
         switch(ch) {
            case ' ':
            case ']':
               return buf.toString();
            case '[':
            case '<':
            case '>':
            case '&':
            case '/':
               return "";
            default:
               if(ch < 0x20 || Character.isWhitespace(ch)) {
                  return "";
               } else {
                  buf.append(ch);
               }
         }
      }
      return "";
   }

   static void parse(final String str, final Handler handler) {
      if(str == null) {
         return;
      }

      StringBuilder buf = new StringBuilder();
      HandlerParseState state = HandlerParseState.TEXT;
      Shortcode enclosingCode = null;
      String currContent = null;
      int pos = 0;
      char[] chars = str.toCharArray();

      while(pos < chars.length) {
         char ch = chars[pos++];
         switch(state) {
            case TEXT:
               switch(ch) {
                  case '[':
                     if(buf.length() > 0) {
                        handler.text(buf.toString());
                        buf.setLength(0);
                     }
                     Shortcode.Type type = handler.type(scanForName(pos, chars));
                     switch(type) {
                        case SELF_CLOSING:
                           state = HandlerParseState.SELF_CLOSING;
                           break;
                        case ENCLOSING:
                           state = HandlerParseState.START_ENCLOSING;
                           break;
                     }
                     break;
               }
               buf.append(ch);
               break;
            case SELF_CLOSING:
               switch(ch) {
                  case ']':
                     buf.append(ch);
                     try {
                        handler.shortcode(ShortcodeParser.parseStart(buf.toString()));
                        buf.setLength(0);
                     } catch(ParseException pe) {
                        pe.printStackTrace();
                        handler.parseError(buf.toString(), pe);
                     }
                     state = HandlerParseState.TEXT;
                     break;
                  default:
                     buf.append(ch);
                     break;
               }
               break;
            case START_ENCLOSING:
               switch(ch) {
                  case ']':
                     try {
                        buf.append(ch);
                        enclosingCode = ShortcodeParser.parseStart(buf.toString());
                        state = HandlerParseState.CONTENT;
                     } catch(ParseException pe) {
                        handler.parseError(buf.toString(), pe);
                        state = HandlerParseState.TEXT;
                     }
                     buf.setLength(0);
                     break;
                  default:
                     buf.append(ch);
                     break;
               }
               break;
            case CONTENT:
               switch(ch) {
                  case '[':
                     currContent = buf.toString();
                     buf.setLength(0);
                     state = HandlerParseState.START_END;
                     break;
                  default:
                     buf.append(ch);
                     break;
               }
               break;
            case START_END:
               switch(ch) {
                  case '/':
                     state = HandlerParseState.END_NAME;
                     break;
                  default:
                     buf.append('[').append(ch);
                     handler.parseError(buf.toString(), null);
                     buf.setLength(0);
                     enclosingCode = null;
                     state = HandlerParseState.TEXT;
                     break;
               }
               break;
            case END_NAME:
               switch(ch) {
                  case ']':
                     if(buf.toString().equals(enclosingCode.name)) {
                        handler.shortcode(enclosingCode.withContent(currContent));
                        buf.setLength(0);
                        enclosingCode = null;
                        currContent = null;
                     } else {
                        handler.parseError(enclosingCode.toString() + currContent + "[/" + buf.toString() + "]", new ParseException("Invalid end tag", 0));
                        buf.setLength(0);
                     }
                     state = HandlerParseState.TEXT;
                     break;
                  default:
                     buf.append(ch);
                     break;
               }
         }
      }

      switch(state) {
         case TEXT:
            if(buf.length() > 0) {
               handler.text(buf.toString());
            }
            break;
         default:
            handler.parseError(buf.toString(), null);
            break;
      }
   }
}
