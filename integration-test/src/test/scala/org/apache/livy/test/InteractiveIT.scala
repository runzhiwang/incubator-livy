/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.test

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.scalatest.concurrent.Eventually._
import org.scalatest.OptionValues._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.livy.rsc.RSCConf
import org.apache.livy.sessions._
import org.apache.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class InteractiveIT extends BaseIntegrationTestSuite {
  private def withNewSession[R] (
      kind: Kind,
      sparkConf: Map[String, String] = Map.empty,
      waitForIdle: Boolean = true,
      heartbeatTimeoutInSecond: Int = 0)
    (f: (LivyRestClient#InteractiveSession) => R): R = {
    withSession(livyClient.startSession(None, kind, sparkConf, heartbeatTimeoutInSecond)) { s =>
      if (waitForIdle) {
        s.verifySessionIdle()
      }
      f(s)
    }
  }

  private def startsWith(result: String): String = Pattern.quote(result) + ".*"

  private def literal(result: String): String = Pattern.quote(result)
}
