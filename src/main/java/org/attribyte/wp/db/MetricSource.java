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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * A source for metrics.
 * <p>
 *    Allows caller to replace implementation (e.g. use HDR timer).
 * </p>
 */
public interface MetricSource {

   /**
    * Creates a new timer.
    * @return The new timer.
    */
   public Timer newTimer();

   /**
    * Creates a new meter.
    * @return The new meter.
    */
   public Meter newMeter();


   public static final MetricSource DEFAULT = new MetricSource() {
      @Override
      public Timer newTimer() {
         return new Timer();
      }

      @Override
      public Meter newMeter() {
         return new Meter();
      }
   };
}