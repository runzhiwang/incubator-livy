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

import java.io.File
import java.util.UUID

import scala.language.postfixOps

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues._

import org.apache.livy.sessions.SessionState
import org.apache.livy.test.apps._
import org.apache.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class BatchIT extends BaseIntegrationTestSuite with BeforeAndAfterAll {
  private var testLibPath: String = _

  protected override def beforeAll() = {
    super.beforeAll()
    testLibPath = uploadToHdfs(new File(testLib))
  }

  private def newOutputPath(): String = {
    cluster.hdfsScratchDir().toString() + "/" + UUID.randomUUID().toString()
  }

  private def uploadResource(name: String): String = {
    val hdfsPath = new Path(cluster.hdfsScratchDir(), UUID.randomUUID().toString + "-" + name)
    val in = getClass.getResourceAsStream("/" + name)
    val out = cluster.fs.create(hdfsPath)
    try {
      IOUtils.copy(in, out)
    } finally {
      in.close()
      out.close()
    }
    hdfsPath.toUri().getPath()
  }

  private def withScript[R]
    (scriptPath: String, args: List[String], sparkConf: Map[String, String] = Map.empty)
    (f: (LivyRestClient#BatchSession) => R): R = {
    val s = livyClient.startBatch(None, scriptPath, None, args, sparkConf)
    withSession(s)(f)
  }

  private def withTestLib[R]
    (testClass: Class[_], args: List[String], sparkConf: Map[String, String] = Map.empty)
    (f: (LivyRestClient#BatchSession) => R): R = {
    val s = livyClient.startBatch(None, testLibPath, Some(testClass.getName()), args, sparkConf)
    withSession(s)(f)
  }
}
