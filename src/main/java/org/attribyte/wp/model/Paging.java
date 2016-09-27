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

import org.joda.time.Interval;

/**
 * Paging constraint.
 */
public class Paging {

   /**
    * Creates paging within an interval.
    * @param start The start index.
    * @param limit The maximum items returned.
    * @param interval The interval.
    * @param startIsOpen Is the start of the interval open, if specified.
    * @param endIsOpen Is the end of the interval open, if specified.
    */
   public Paging(final int start, final int limit, final Interval interval,
                 final boolean startIsOpen, final boolean endIsOpen) {
      this.start = start;
      this.limit = limit;
      this.interval = interval;
      this.startIsOpen = startIsOpen;
      this.endIsOpen = endIsOpen;
   }

   /**
    * Creates paging within an interval.
    * @param start The start index.
    * @param limit The maximum items returned.
    * @param interval The interval.
    */
   public Paging(final int start, final int limit, final Interval interval) {
      this(start, limit, interval, false, false);
   }

   /**
    * Creates simple paging.
    * @param start The start index.
    * @param limit The maximum items returned.
    */
   public Paging(final int start, final int limit) {
      this(start, limit, null);
   }

   /**
    * Creates paging with an open start.
    * @param openStart Is the start of the interval open?
    * @return The modified paging.
    */
   public Paging withOpenStart(final boolean openStart) {
      return new Paging(start, limit, interval, openStart, endIsOpen);
   }

   /**
    * Creates paging with an open end.
    * @param openEnd Is the end of the interval open?
    * @return The modified paging.
    */
   public Paging withOpenEnd(final boolean openEnd) {
      return new Paging(start, limit, interval, startIsOpen, openEnd);
   }

   /**
    * The start index.
    */
   public final int start;

   /**
    * The maximum items returned.
    */
   public final int limit;

   /**
    * The time interval, if any.
    */
   public final Interval interval;

   /**
    * The start timestamp of this interval is open (excluded). Default {@code false}.
    */
   public final boolean startIsOpen;

   /**
    * The end timestamp of this interval is open (excluded). Default {@code false}.
    */
   public final boolean endIsOpen;
}
