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

import com.google.common.collect.Lists;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;

import java.text.ParseException;
import java.util.List;

public class BlockParser {

   /**
    * Parses content as a sequence of blocks.
    * @param content The content to parse.
    * @param baseUri The base URI for content.
    * @return The sequence of blocks.
    */
   public static List<Block> parse(final String content, final String baseUri) throws ParseException {
      Document doc = Jsoup.parseBodyFragment(content, baseUri);

      List<Block> blocks = Lists.newArrayList();
      Block currBlock = null;
      List<Node> currNodes = Lists.newArrayList();

      for(Node node : doc.body().childNodes()) {
         if(node.nodeName().equals("#comment")) {
            String comment = commentContent(node);
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

      return blocks;
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

   /**
    * Gets the content of a comment node.
    * @param comment The comment node.
    * @return The content.
    */
   private static String commentContent(final Node comment) {
      String content = comment.toString().trim();
      if(content.startsWith("<!--")) {
         content = content.substring(4);
      }
      if(content.endsWith("-->")) {
         content = content.substring(0, content.lastIndexOf("-->"));
      }
      return content.trim();
   }

}
