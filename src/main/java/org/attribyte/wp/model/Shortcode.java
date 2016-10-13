package org.attribyte.wp.model;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

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
    * Escapes an attribute value.
    * @param val The value.
    * @return The escaped value.
    */
   public static String escapeAttribute(final String val) {
      return Strings.nullToEmpty(val); //TODO?
   }
}
