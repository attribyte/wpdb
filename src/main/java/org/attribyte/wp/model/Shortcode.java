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
    * The type (enclosing or self-closing).
    */
   public enum Type {

      /**
       * Self-closing (@code{[example]}).
       */
      SELF_CLOSING,

      /**
       * Enclosing (@code{[example]content[/example]}).
       */
      ENCLOSING,

      /**
       * Unknown or invalid type.
       */
      UNKNOWN;
   }

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
    * Gets an attribute value.
    * @param name The attribute name.
    * @return The value or {@code null} if no value for this attribute.
    */
   public final String value(final String name) {
      return attributes.get(name.toLowerCase());
   }

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
            appendAttributeValue(kv.getValue(), buf);
         } else {
            buf.append(" ").append(kv.getKey()).append("=");
            appendAttributeValue(kv.getValue(), buf);
         }
      });
      buf.append("]");
      if(content != null) {
         buf.append(content);
         buf.append("[/").append(name).append("]");
      }
      return buf.toString();
   }

   /**
    * Appends an attribute value with appropriate quoting.
    * @param value The value.
    * @param buf The buffer to append to.
    * @return The input buffer.
    */
   private StringBuilder appendAttributeValue(final String value, final StringBuilder buf) {
      if(value.contains("\"")) {
         buf.append("\'").append(escapeAttribute(value)).append("\'");
      } else if(value.contains(" ") || value.contains("\'") || value.contains("=")) {
         buf.append("\"").append(escapeAttribute(value)).append("\"");
      } else {
         buf.append(escapeAttribute(value));
      }
      return buf;
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

      Shortcode startTag = ShortcodeParser.parseStart(exp.substring(0, end + 1));

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
    * Parse an arbitrary string.
    * @param str The string.
    * @param handler The handler for parse events.
    */
   public static void parse(final String str, final ShortcodeParser.Handler handler) {
      ShortcodeParser.parse(str, handler);
   }

   /**
    * Escapes an attribute value.
    * @param val The value.
    * @return The escaped value.
    */
   private static String escapeAttribute(final String val) {
      return Strings.nullToEmpty(val); //TODO?
   }
}
