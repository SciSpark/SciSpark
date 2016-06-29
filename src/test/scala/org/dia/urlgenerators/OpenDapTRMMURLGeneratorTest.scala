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
package org.dia.urlgenerators

import org.dia.urlgenerators.OpenDapTRMMURLGenerator
import org.scalatest.FunSuite 
import java.nio.file.{ Paths, Files }

/**
 * Tests whether the OpenDapTRMM URLs creator works.
 */
class OpenDapTRMMURLGeneratorTest extends FunSuite {

  test("testLinkGenerationDaily") {
    val checkLink = false
    OpenDapTRMMURLGenerator.run(checkLink, "testLinkfileD.txt", "200101010000", "200101310000", 1, List("precipitation"))
    if (Files.exists(Paths.get("testLinkfileD.txt")) && Files.size(Paths.get("testLinkfileD.txt")) != 0){
      assert(true)
    }else {
      assert(false)
    }
  }

  test("testLinkGeneration3Hrly") {
    val checkLink = false
    OpenDapTRMMURLGenerator.run(checkLink, "testLinkfileH.txt",  "201001010000",   "201001031500", 2, List("precipitation")) 
    if (Files.exists(Paths.get("testLinkfileH.txt")) && Files.size(Paths.get("testLinkfileH.txt")) != 0){
      assert(true)
    }else {
      assert(false)
    }
  }

  test("testLinkGenerationDailyWithSelection") {
    val checkLink = false
    OpenDapTRMMURLGenerator.run(checkLink, "testLinkfileDsub.txt", "200103010000", "200103310000", 1 , List("data,1,399,1,1439"))
    if (Files.exists(Paths.get("testLinkfileDsub.txt")) && Files.size(Paths.get("testLinkfileDsub.txt")) != 0){
      assert(true)
    }else {
      assert(false)
    }
  }

  test("testLinkGeneration3HrlyWithSelection") {
    val checkLink = true
    OpenDapTRMMURLGenerator.run(checkLink, "testLinkfileHsub.txt",  "201006150000",   "201006161500", 2 , List("precipitation,1,1439,1,399","nlon,1,1439","nlat,1,399")) //"201003010300", "201003031500", 2)
    if (Files.exists(Paths.get("testLinkfileHsub.txt")) && Files.size(Paths.get("testLinkfileHsub.txt")) != 0){
      assert(true)
    }else {
      assert(false)
    }
  }

}
