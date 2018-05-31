/*
 * Copyright 2018 Attribyte, LLC
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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.jsoup.nodes.Node;

import java.util.Collection;

/**
 * A block.
 * See: <a href="https://wordpress.org/gutenberg/handbook/language/">https://wordpress.org/gutenberg/handbook/language/</a>.
 */
public class Block {

   /**
    * Creates a block with empty attributes.
    * @param type The block type.
    * @param nodes The content as a collection of nodes.
    */
   public Block(final String type, final Collection<Node> nodes) {
      this(type, "", nodes);
   }

   /**
    * Creates an empty block with no attributes.
    * @param type The block type.
    */
   public Block(final String type) {
      this.type = type;
      this.attributes = "";
      this.content = ImmutableList.of();
   }

   /**
    * Creates an empty block with attributes.
    * @param type The block type.
    * @param attributes The attributes string.
    */
   public Block(final String type, final String attributes) {
      this.type = type;
      this.attributes = attributes != null ? attributes : "";
      this.content = ImmutableList.of();
   }

   /**
    * Creates a block with attributes.
    * @param type The block type.
    * @param attributes The attributes string.
    * @param nodes The content as a collection of nodes.
    */
   public Block(final String type, final String attributes, final Collection<Node> nodes) {
      this.type = type;
      this.attributes = attributes != null ? attributes : "";
      this.content = nodes != null ? ImmutableList.copyOf(nodes) : ImmutableList.of();
   }

   /**
    * Adds content to this block.
    * @param nodes The collection of nodes that make up the content.
    * @return This block with content nodes added.
    */
   public Block withContent(final Collection<Node> nodes) {
      return new Block(type, attributes, nodes);
   }

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("type", type)
              .add("attributes", attributes)
              .add("content", content)
              .toString();
   }

   /**
    * The name.
    */
   public final String type;

   /**
    * The attributes as a string.
    */
   public final String attributes;

   /**
    * The content of the block as an immutable list of nodes.
    */
   public final ImmutableList<Node> content;
}
