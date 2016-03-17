/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia

import java.util.{ Calendar, Date }
import com.joestelmach.natty.Parser

/**
 * Contains a set of parse methods relevant to the entire SciSpark source tree
 */
object Parsers {

  def parseDateFromString(Name: String): Date = {
    val Parser = new Parser
    val DataGroups = Parser.parse(Name)
    if (DataGroups.size() > 0) {
      val calendar = Calendar.getInstance
      calendar.setTime(DataGroups.get(0).getDates.get(0))
      calendar.getTime
    } else {
      null
    }
  }

}
