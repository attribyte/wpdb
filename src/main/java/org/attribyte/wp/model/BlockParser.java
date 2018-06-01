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

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Comment;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeVisitor;

import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class BlockParser {

   /**
    * Parses content as a sequence of blocks.
    * @param content The content to parse.
    * @param baseUri The base URI for content.
    * @return The sequence of blocks.
    */
   public static List<Block> parse(final String content, final String baseUri) throws ParseException {
      Document doc = Jsoup.parseBodyFragment(content, baseUri);
      List<Node> blockMarkers = Lists.newArrayList();
      doc.body().traverse(new NodeVisitor() {
         @Override
         public void head(final Node node, final int i) {
            if(node instanceof Comment) {
               String comment = Strings.nullToEmpty(((Comment)node).getData()).trim();
               if(comment.startsWith("wp:") || comment.startsWith("/wp:")) {
                  blockMarkers.add(node);
               }
            }
         }

         @Override
         public void tail(final Node node, final int i) {

         }
      });

      //Parse/tidy of invalid markup may leave markers as children
      //of nested elements. Move them to the body...

      for(int i = 0; i < blockMarkers.size(); i++) {
         Node node = blockMarkers.get(i);
         if(node.parent() != doc.body()) {
            Node cloneNode = node.clone();
            node.remove();
            if(i == 0) {
               doc.body().prependChild(cloneNode);
            } else {
               blockMarkers.get(i-1).after(cloneNode);
            }
            blockMarkers.set(i, cloneNode);
         }
      }

      Set<Node> markerSet = Sets.newIdentityHashSet();
      markerSet.addAll(blockMarkers);

      List<Block> blocks = Lists.newArrayList();
      Block currBlock = null;
      List<Node> currNodes = Lists.newArrayList();

      for(Node node : doc.body().childNodes()) {
         if(markerSet.contains(node)) {
            String comment = ((Comment)node).getData().trim();
            if(comment.startsWith("wp:")) {
               if(!currNodes.isEmpty()) {
                  //Default block...
                  blocks.add(new Block("html", currNodes));
                  currNodes.clear();
               }
               currBlock = block(comment, 3);
            } else if(comment.startsWith("/wp:")) {
               Block checkBlock = block(comment, 4);
               if(currBlock == null) {
                  throw new ParseException(String.format("Missing start block for 'wp:%s'", checkBlock.type), 0);
               } else if(!currBlock.type.equals(checkBlock.type)) {
                  throw new ParseException(String.format("Start/end block mismatch. Expecting '/wp:%s' but found '/wp:%s",
                          currBlock.type, checkBlock.type), 0);
               } else {
                  blocks.add(currBlock.withContent(currNodes));
                  currBlock = null;
                  currNodes.clear();
               }
            } else {
               currNodes.add(node);
            }
         } else {
            currNodes.add(node);
         }
      }

      if(currBlock != null) {
         throw new ParseException(String.format("Expecting end block for 'wp:%s'", currBlock.type), 0);
      }

      if(!currNodes.isEmpty()) {
         blocks.add(new Block("html", currNodes));
      }

      return removeEmptyBlocks(blocks);
   }

   /**
    * Removes empty blocks from a collection of blocks.
    * @param blocks The collection of blocks.
    * @return A list of blocks with empty blocks removed.
    */
   public static List<Block> removeEmptyBlocks(final Collection<Block> blocks) {
      List<Block> cleanBlocks = Lists.newArrayListWithExpectedSize(blocks.size());
      for(Block block : blocks) {
         if(block.content.isEmpty()) {
            continue;
         }

         if(isWhitespace(block.content)) {
            continue;
         }

         cleanBlocks.add(block);
      }
      return cleanBlocks;
   }

   /**
    * @param nodes A collection of nodes.
    * @return Are all nodes whitespace-only?
    */
   private static boolean isWhitespace(final Collection<Node> nodes) {
      for(Node node : nodes) {
         if(!isWhitespace(node)) {
            return false;
         }
      }
      return true;
   }

   /**
    * @param node A node.
    * @return Is the node whitespace-only?
    */
   private static boolean isWhitespace(final Node node) {
      if(node instanceof TextNode) {
         return CharMatcher.whitespace().matchesAllOf(((TextNode)node).text());
      } else if(node instanceof Element) {
         boolean hasAttributes = node.attributes().size() > 0;
         return CharMatcher.whitespace().matchesAllOf(((Element)node).text()) && !hasAttributes;
      } else {
         return false;
      }
   }

   /**
    * Creates an empty block from a comment string.
    * @param comment The comment string.
    * @param nameStart The start index for the name.
    * @return The block.
    */
   private static Block block(final String comment, final int nameStart) {
      int attrIndex = comment.indexOf('{');
      if(attrIndex != -1) {
         return new Block(comment.substring(nameStart, attrIndex).trim(),
                 comment.substring(attrIndex).trim());
      } else {
         return new Block(comment.substring(nameStart).trim());
      }
   }
}
