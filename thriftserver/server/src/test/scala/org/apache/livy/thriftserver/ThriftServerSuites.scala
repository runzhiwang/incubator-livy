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

package org.apache.livy.thriftserver

import java.sql.{Connection, Date, SQLException, Statement, Timestamp, Types}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.hive.jdbc.HiveStatement

import org.apache.livy.LivyConf


trait CommonThriftTests {
  def hiveSupportEnabled(sparkMajorVersion: Int, livyConf: LivyConf): Boolean = {
    sparkMajorVersion > 1 || livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
  }

  def dataTypesTest(statement: Statement, mapSupported: Boolean): Unit = {
    val resultSet = statement.executeQuery(
      "select cast(1 as tinyint)," +
        "cast(2 as smallint)," +
        "cast(3 as int)," +
        "cast(4 as bigint)," +
        "cast(5.5 as float)," +
        "cast(6.6 as double)," +
        "cast(7.7 as decimal(10, 1))," +
        "cast(true as boolean)," +
        "cast('123' as binary)," +
        "cast('string_val' as string)," +
        "cast('varchar_val' as varchar(20))," +
        "cast('char_val' as char(20))," +
        "cast('2018-08-06 09:11:15' as timestamp)," +
        "cast('2018-08-06' as date)")

    val rsMetaData = resultSet.getMetaData()

    resultSet.next()

    assert(resultSet.getByte(1) == 1)
    assert(rsMetaData.getColumnTypeName(1) == "tinyint")

    assert(resultSet.getShort(2) == 2)
    assert(rsMetaData.getColumnTypeName(2) == "smallint")

    assert(resultSet.getInt(3) == 3)
    assert(rsMetaData.getColumnTypeName(3) == "int")

    assert(resultSet.getLong(4) == 4)
    assert(rsMetaData.getColumnTypeName(4) == "bigint")

    assert(resultSet.getFloat(5) == 5.5)
    assert(rsMetaData.getColumnTypeName(5) == "float")

    assert(resultSet.getDouble(6) == 6.6)
    assert(rsMetaData.getColumnTypeName(6) == "double")

    assert(resultSet.getBigDecimal(7).doubleValue() == 7.7)
    assert(rsMetaData.getColumnTypeName(7) == "decimal")

    assert(resultSet.getBoolean(8) == true)
    assert(rsMetaData.getColumnTypeName(8) == "boolean")

    val resultBytes = Source.fromInputStream(resultSet.getBinaryStream(9))
      .map(_.toByte).toArray
    assert("123".getBytes.sameElements(resultBytes))
    assert(rsMetaData.getColumnTypeName(9) == "binary")

    assert(resultSet.getString(10) == "string_val")
    assert(rsMetaData.getColumnTypeName(10) == "string")

    assert(resultSet.getString(11) == "varchar_val")
    assert(rsMetaData.getColumnTypeName(11) == "string")

    assert(resultSet.getString(12) == "char_val")
    assert(rsMetaData.getColumnTypeName(12) == "string")

    assert(resultSet.getTimestamp(13).
      compareTo(Timestamp.valueOf("2018-08-06 09:11:15")) == 0)
    assert(rsMetaData.getColumnTypeName(13) == "timestamp")

    assert(resultSet.getDate(14).
      compareTo(Date.valueOf("2018-08-06")) == 0)
    assert(rsMetaData.getColumnTypeName(14) == "date")

    assert(!resultSet.next())

    val resultSetWithNulls = statement.executeQuery(
      "select cast(null as tinyint), " +
        "cast(null as smallint)," +
        "cast(null as int)," +
        "cast(null as bigint)," +
        "cast(null as float)," +
        "cast(null as double)," +
        "cast(null as decimal)," +
        "cast(null as boolean)," +
        "cast(null as binary)," +
        "cast(null as string)," +
        "cast(null as varchar(20))," +
        "cast(null as char(20))," +
        "cast(null as timestamp)," +
        "cast(null as date)")

    resultSetWithNulls.next()

    assert(resultSetWithNulls.getByte(1) == 0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getShort(2) == 0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getInt(3) == 0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getLong(4) == 0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getFloat(5) == 0.0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getDouble(6) == 0.0)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getBigDecimal(7) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getBoolean(8) == false)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getBinaryStream(9) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getString(10) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getString(11) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getString(12) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getTimestamp(13) == null)
    assert(resultSetWithNulls.wasNull())

    assert(resultSetWithNulls.getDate(14) == null)
    assert(resultSetWithNulls.wasNull())

    assert(!resultSetWithNulls.next())

    val complexTypesQuery = if (mapSupported) {
      "select array(1.5, 2.4, 1.3), struct('a', 1, 1.5), map(1, 'a', 2, 'b')"
    } else {
      "select array(1.5, 2.4, 1.3), struct('a', 1, 1.5)"
    }

    val resultSetComplex = statement.executeQuery(complexTypesQuery)
    resultSetComplex.next()
    assert(resultSetComplex.getString(1) == "[1.5,2.4,1.3]")
    assert(resultSetComplex.getString(2) == "{\"col1\":\"a\",\"col2\":1,\"col3\":1.5}")
    if (mapSupported) {
      assert(resultSetComplex.getString(3) == "{1:\"a\",2:\"b\"}")
    }
    assert(!resultSetComplex.next())
  }

  def getSchemasTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val schemaResultSet = metadata.getSchemas()
    assert(schemaResultSet.getMetaData.getColumnCount == 2)
    assert(schemaResultSet.getMetaData.getColumnName(1) == "TABLE_SCHEM")
    assert(schemaResultSet.getMetaData.getColumnName(2) == "TABLE_CATALOG")
    schemaResultSet.next()
    assert(schemaResultSet.getString(1) == "default")
    assert(!schemaResultSet.next())
  }

  def getFunctionsTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData

    val functionResultSet = metadata.getFunctions("", "default", "unix_timestamp")
    assert(functionResultSet.getMetaData.getColumnCount == 6)
    assert(functionResultSet.getMetaData.getColumnName(1) == "FUNCTION_CAT")
    assert(functionResultSet.getMetaData.getColumnName(2) == "FUNCTION_SCHEM")
    assert(functionResultSet.getMetaData.getColumnName(3) == "FUNCTION_NAME")
    assert(functionResultSet.getMetaData.getColumnName(4) == "REMARKS")
    assert(functionResultSet.getMetaData.getColumnName(5) == "FUNCTION_TYPE")
    assert(functionResultSet.getMetaData.getColumnName(6) == "SPECIFIC_NAME")
    functionResultSet.next()
    assert(functionResultSet.getString(3) == "unix_timestamp")
    assert(functionResultSet.getString(6) ==
      "org.apache.spark.sql.catalyst.expressions.UnixTimestamp")
    assert(!functionResultSet.next())
  }

  def getTablesTest(connection: Connection): Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute("CREATE TABLE test_get_tables (id integer, desc string) USING json")
      val metadata = connection.getMetaData
      val tablesResultSet = metadata.getTables("", "default", "*", Array("TABLE"))
      assert(tablesResultSet.getMetaData.getColumnCount == 5)
      assert(tablesResultSet.getMetaData.getColumnName(1) == "TABLE_CAT")
      assert(tablesResultSet.getMetaData.getColumnName(2) == "TABLE_SCHEM")
      assert(tablesResultSet.getMetaData.getColumnName(3) == "TABLE_NAME")
      assert(tablesResultSet.getMetaData.getColumnName(4) == "TABLE_TYPE")
      assert(tablesResultSet.getMetaData.getColumnName(5) == "REMARKS")

      tablesResultSet.next()
      assert(tablesResultSet.getString(3) == "test_get_tables")
      assert(tablesResultSet.getString(4) == "TABLE")
      assert(!tablesResultSet.next())
    } finally {
      statement.execute("DROP TABLE IF EXISTS test_get_tables")
      statement.close()
    }

  }

  def getColumnsTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val statement = connection.createStatement()
    try {
      statement.execute("CREATE TABLE test_get_columns (id integer, desc string) USING json")

      val columnsResultSet = metadata.getColumns("", "default", "test_get_columns", ".*")
      assert(columnsResultSet.getMetaData.getColumnCount == 23)
      columnsResultSet.next()
      assert(columnsResultSet.getString(1) == "")
      assert(columnsResultSet.getString(2) == "default")
      assert(columnsResultSet.getString(3) == "test_get_columns")
      assert(columnsResultSet.getString(4) == "id")
      assert(columnsResultSet.getInt(5) == 4)
      assert(columnsResultSet.getString(6) == "integer")
      assert(columnsResultSet.getInt(7) == 10)
      assert(columnsResultSet.getString(8) == null)
      assert(columnsResultSet.getInt(9) == 0)
      assert(columnsResultSet.getInt(10) == 10)
      assert(columnsResultSet.getInt(11) == 1)
      assert(columnsResultSet.getString(12) == "")
      assert(columnsResultSet.getString(13) == null)
      assert(columnsResultSet.getString(14) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getInt(17) == 0)
      assert(columnsResultSet.getString(18) == "YES")
      assert(columnsResultSet.getString(19) == null)
      assert(columnsResultSet.getString(20) == null)
      assert(columnsResultSet.getString(21) == null)
      assert(columnsResultSet.getString(22) == null)
      assert(columnsResultSet.getString(23) == "NO")
      columnsResultSet.next()
      assert(columnsResultSet.getString(1) == "")
      assert(columnsResultSet.getString(2) == "default")
      assert(columnsResultSet.getString(3) == "test_get_columns")
      assert(columnsResultSet.getString(4) == "desc")
      assert(columnsResultSet.getInt(5) == 12)
      assert(columnsResultSet.getString(6) == "string")
      assert(columnsResultSet.getInt(7) == Integer.MAX_VALUE)
      assert(columnsResultSet.getString(8) == null)
      assert(columnsResultSet.getString(9) == null)
      assert(columnsResultSet.getString(10) == null)
      assert(columnsResultSet.getInt(11) == 1)
      assert(columnsResultSet.getString(12) == "")
      assert(columnsResultSet.getString(13) == null)
      assert(columnsResultSet.getString(14) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getString(16) == null)
      assert(columnsResultSet.getInt(17) == 1)
      assert(columnsResultSet.getString(18) == "YES")
      assert(columnsResultSet.getString(19) == null)
      assert(columnsResultSet.getString(20) == null)
      assert(columnsResultSet.getString(21) == null)
      assert(columnsResultSet.getString(22) == null)
      assert(columnsResultSet.getString(23) == "NO")
      assert(!columnsResultSet.next())
    } finally {
      statement.execute("DROP TABLE IF EXISTS test_get_columns")
      statement.close()
    }

  }

  def operationLogRetrievalTest(statement: Statement): Unit = {
    statement.execute("select 1")
    val logIterator = statement.asInstanceOf[HiveStatement].getQueryLog().iterator()
    statement.close()

    // Only execute statement support operation log retrieval and it only produce one log
    assert(logIterator.next() ==
      "Livy session has not yet started. Please wait for it to be ready...")
    assert(!logIterator.hasNext)
  }

  def getCatalogTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val catalogResultSet = metadata.getCatalogs()
    // Spark doesn't support getCatalog. In current implementation, it's a no-op and does not return
    // any data
    assert(!catalogResultSet.next())
  }

  def getTableTypeTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val tableTypesResultSet = metadata.getTableTypes()
    tableTypesResultSet.next()
    assert(tableTypesResultSet.getString(1) == "TABLE")
    tableTypesResultSet.next()
    assert(tableTypesResultSet.getString(1) == "VIEW")
    assert(!tableTypesResultSet.next())
  }

  def getTypeInfoTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val typeInfoResultSet = metadata.getTypeInfo()
    val expectResults = Array(
      ("void", Types.NULL, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0),
      ("boolean", Types.BOOLEAN, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("byte", Types.TINYINT, 3, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("short", Types.SMALLINT, 5, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("integer", Types.INTEGER, 10, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("long", Types.BIGINT, 19, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("float", Types.FLOAT, 7, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("double", Types.DOUBLE, 15, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("date", Types.DATE, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("timestamp", Types.TIMESTAMP, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("string", Types.VARCHAR, 0, 1, true, 3, true, false, false, null, 0, 0, 0, 0, 0),
      ("binary", Types.BINARY, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("decimal", Types.DECIMAL, 38, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("array", Types.ARRAY, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("map", Types.OTHER, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0),
      ("struct", Types.STRUCT, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("udt", Types.OTHER, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0)
    )
    for (expect <- expectResults) {
      typeInfoResultSet.next()
      assert(typeInfoResultSet.getString(1) == expect._1)
      assert(typeInfoResultSet.getInt(2) == expect._2)
      assert(typeInfoResultSet.getInt(3) == expect._3)
      assert(typeInfoResultSet.getString(4) == null)
      assert(typeInfoResultSet.getString(5) == null)
      assert(typeInfoResultSet.getString(6) == null)
      assert(typeInfoResultSet.getShort(7) == expect._4)
      assert(typeInfoResultSet.getBoolean(8) == expect._5)
      assert(typeInfoResultSet.getShort(9) == expect._6)
      assert(typeInfoResultSet.getBoolean(10) == expect._7)
      assert(typeInfoResultSet.getBoolean(11) == expect._8)
      assert(typeInfoResultSet.getBoolean(12) == expect._9)
      assert(typeInfoResultSet.getString(13) == expect._10)
      assert(typeInfoResultSet.getShort(14) == expect._11)
      assert(typeInfoResultSet.getShort(15) == expect._12)
      assert(typeInfoResultSet.getInt(16) == expect._13)
      assert(typeInfoResultSet.getInt(17) == expect._14)
      assert(typeInfoResultSet.getInt(18) == expect._15)
    }
    assert(!typeInfoResultSet.next())
  }
}

class BinaryThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.binary
  override def port: Int = 20000
}

class HttpThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.http
  override def port: Int = 20001
}
