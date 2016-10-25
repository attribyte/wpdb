package org.attribyte.wp.model;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A shortcode.
 * See: <a href="https://codex.wordpress.org/Shortcode_API">https://codex.wordpress.org/Shortcode_API</a>.
 */
public class Shortcode {

   /**
    * Creates a shortcode without content.
    * @param name The name.
    * @param attributes The attributes.
    */
   public Shortcode(final String name, final Map<String, String> attributes) {
      this(name, attributes, null);
   }

   /**
    * Creates a shortcode with content.
    * @param name The name.
    * @param attributes The attributes.
    * @param content The content.
    */
   public Shortcode(final String name, final Map<String, String> attributes,
                    final String content) {
      this.name = name;
      this.attributes = attributes != null ? ImmutableMap.copyOf(attributes) : ImmutableMap.of();
      this.content = Strings.emptyToNull(content);
   }

   /**
    * Adds content to a shortcode.
    * @param content The content.
    * @return The shortcode with content added.
    */
   public Shortcode withContent(final String content) {
      return new Shortcode(name, attributes, content);
   }

   /**
    * The name.
    */
   public final String name;

   /**
    * The attributes.
    */
   public final ImmutableMap<String, String> attributes;

   /**
    * The content.
    */
   public final String content;

   /**
    * Gets a positional attribute value.
    * @param pos The position.
    * @return The value, or {@code null} if no value at the position.
    */
   public final String positionalValue(final int pos) {
      return attributes.get(String.format("$%d", pos));
   }

   /**
    * Gets a list of any positional values.
    * @return The list of values.
    */
   public final List<String> positionalValues() {
      return attributes.entrySet()
              .stream()
              .filter(kv -> kv.getKey().startsWith("$"))
              .map(Map.Entry::getValue)
              .collect(Collectors.toList());
   }

   @Override
   public final String toString() {
      StringBuilder buf = new StringBuilder("[");
      buf.append(name);
      attributes.entrySet().forEach(kv -> {
         if(kv.getKey().startsWith("$")) {
            buf.append(" ");
            if(kv.getValue().contains(" ")) {
               buf.append("\"").append(escapeAttribute(kv.getValue())).append("\"");
            } else {
               buf.append(kv.getValue());
            }
         } else {
            buf.append(" ").append(kv.getKey()).append("=\"").append(escapeAttribute(kv.getValue())).append("\"");
         }
      });
      buf.append("]");
      if(content != null) {
         buf.append(content);
         buf.append("[/").append(name).append("]");
      }
      return buf.toString();
   }


   public static interface Handler {
      public void shortcode(Shortcode shortcode);
      public void text(String text);
      public void parseError(String text, ParseException pe);
      public boolean expectContent(final String shortcode);
   }

   private enum HandlerParseState {
      TEXT,
      START,
      CONTENT,
      START_END,
      END_NAME;
   }


   public static void parse(final String str, final Handler handler) {
      if(str == null) {
         return;
      }

      StringBuilder buf = new StringBuilder();
      HandlerParseState state = HandlerParseState.TEXT;
      Shortcode currCode = null;
      String currContent = null;
      for(char ch : str.toCharArray()) {
         switch(state) {
            case TEXT:
               switch(ch) {
                  case '[':
                     if(buf.length() > 0) {
                        handler.text(buf.toString());
                        buf.setLength(0);
                     }
                     state = HandlerParseState.START;
                     buf.append(ch);
                     break;
                  default:
                     buf.append(ch);
                     break;
               }
               break;
            case START:
               switch(ch) {
                  case ']':
                     try {
                        buf.append(ch);
                        currCode = Parser.parseStart(buf.toString());
                        if(!handler.expectContent(currCode.name)) {
                           handler.shortcode(currCode);
                           currCode = null;
                           state = HandlerParseState.TEXT;
                        } else {
                           state = HandlerParseState.CONTENT;
                        }
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
                     state = HandlerParseState.TEXT;
                     break;
               }
               break;
            case END_NAME:
               switch(ch) {
                  case ']':
                     if(buf.toString().equals(currCode.name)) {
                        handler.shortcode(currCode.withContent(currContent));
                        buf.setLength(0);
                        currCode = null;
                        currContent = null;
                     } else {
                        handler.parseError(currCode.toString() + currContent + "[/" + buf.toString() + "]", new ParseException("Invalid end tag", 0));
                        buf.setLength(0);
                     }
                     state = HandlerParseState.TEXT;
                     break;
                  default:
                     buf.append(ch);
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

   /**
    * Parses a shortcode
    * @param shortcode The shortcode string.
    * @return The parsed shortcode.
    * @throws ParseException on invalid code.
    */
   public static Shortcode parse(final String shortcode) throws ParseException {
      String exp = shortcode.trim();

      if(exp.length() < 3) {
         throw new ParseException(String.format("Invalid shortcode ('%s')", exp), 0);
      }

      if(exp.charAt(0) != '[') {
         throw new ParseException("Expecting '['", 0);
      }

      int end = exp.indexOf(']');
      if(end == -1) {
         throw new ParseException("Expecting ']", 0);
      }

      Shortcode startTag = Parser.parseStart(exp.substring(0, end + 1));

      end = exp.lastIndexOf("[/");
      if(end > 0) {
         if(exp.endsWith("[/" + startTag.name + "]")) {
            int start = shortcode.indexOf("]");
            return startTag.withContent(exp.substring(start + 1, end));
         } else {
            throw new ParseException("Invalid shortcode end", 0);
         }
      } else {
         return startTag;
      }
   }

   /**
    * Escapes an attribute value.
    * @param val The value.
    * @return The escaped value.
    */
   public static String escapeAttribute(final String val) {
      return Strings.nullToEmpty(val); //TODO?
   }

   private static class Parser {

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
      private static Shortcode parseStart(final String str) throws ParseException {
         String exp = str.trim();

         if(exp.length() < 3) {
            throw new ParseException(String.format("Invalid shortcode ('%s')", str), 0);
         }

         if(exp.charAt(0) != '[') {
            throw new ParseException("Expecting '['", 0);
         }

         if(exp.charAt(exp.length() -1) != ']') {
            throw new ParseException("Expecting ']'", exp.length() - 1);
         }

         exp = exp.substring(1, exp.length() -1).trim();

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
             * Inside a quoted value.
             */
            QUOTED_VALUE,

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

            StringState state = StringState.BEFORE_START;

            while(pos < chars.length) {
               ch = chars[pos++];
               switch(ch) {
                  case '=':
                     switch(state) {
                        case BEFORE_START:
                           state = StringState.VALUE;
                           break;
                        case QUOTED_VALUE:
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
                        case QUOTED_VALUE:
                           buf.append(ch);
                           break;
                        case VALUE:
                           return value();
                     }
                     break;
                  case '\"':
                  case '\'':
                     switch(state) {
                        case BEFORE_START:
                           state = StringState.QUOTED_VALUE;
                           break;
                        case QUOTED_VALUE:
                           return value();
                        case VALUE:
                           throw new ParseException("Unexpected '\"'", pos);
                     }
                     break;
                  default:
                     switch(state) {
                        case BEFORE_START:
                           state = StringState.VALUE;
                           break;
                     }
                     buf.append(ch);
                     break;
               }
            }

            switch(state) {
               case VALUE:
                  return buf.toString();
               case QUOTED_VALUE:
                  throw new ParseException("Expected '\"' or '\''", pos);
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
   }
}
