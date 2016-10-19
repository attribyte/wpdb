package org.attribyte.wp.model;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.text.ParseException;
import java.util.Map;

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

   @Override
   public final String toString() {
      StringBuilder buf = new StringBuilder("[");
      buf.append(name);
      attributes.entrySet().forEach(kv -> {
         buf.append(" ").append(kv.getKey()).append("=\"").append(escapeAttribute(kv.getValue())).append("\"");
      });
      buf.append("]");
      if(content != null) {
         buf.append(content);
         buf.append("[/").append(name).append("]");
      }
      return buf.toString();
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
       * Parse attributes in a shortcode.
       * @param attrString The attribute string.
       * @return The map of attributes. Keys are <em>lower-case</em>.
       * @throws ParseException on invalid shortcode.
       */
      private static Map<String, String> parseAttributes(String attrString) throws ParseException {
         Map<String, String> attributes = Maps.newHashMapWithExpectedSize(4);
         StringBuilder buf = new StringBuilder();
         AttrState state = AttrState.NAME;
         String currName = "";
         for(char ch : attrString.toCharArray()) {
            switch(state) {
               case NAME:
                  switch(ch) {
                     case '=':
                        currName = buf.toString().toLowerCase();
                        buf.setLength(0);
                        state = AttrState.VALUE;
                        break;
                     default:
                        if(isNameCharacter(ch)) {
                           buf.append(ch);
                        }
                        break;
                  }
                  break;
               case VALUE:
                  switch(ch) {
                     case '\"':
                     case '\'':
                        state = AttrState.QUOTED_VALUE;
                        break;
                     case ' ':
                        attributes.put(currName, buf.toString());
                        buf.setLength(0);
                        state = AttrState.NAME;
                        break;
                     case '[':
                     case ']':
                        throw new ParseException(String.format("Invalid character ('%c')", ch), 0);
                     default:
                        buf.append(ch);
                        break;
                  }
                  break;
               case QUOTED_VALUE:
                  switch(ch) {
                     case '\"':
                     case '\'':
                        attributes.put(currName, buf.toString());
                        buf.setLength(0);
                        state = AttrState.NAME;
                        break;
                     case '[':
                     case ']':
                        throw new ParseException(String.format("Invalid character ('%c')", ch), 0);
                     default:
                        buf.append(ch);
                        break;
                  }
                  break;
            }
         }

         switch(state) {
            case NAME:
               currName = buf.toString().trim();
               if(!currName.isEmpty()) {
                  throw new ParseException(String.format("Expecting attribute value for '%s'", currName), 0);
               }
               break;
            case VALUE:
               attributes.put(currName, buf.toString());
               break;
            case QUOTED_VALUE:
               throw new ParseException("Expecting '\"' to end quoted value", 0);
         }
         return attributes;
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
          * Parsing a quoted attribute value.
          */
         QUOTED_VALUE,

         /**
          * Parsing an unquoted attribute value.
          */
         VALUE;
      }
   }
}
