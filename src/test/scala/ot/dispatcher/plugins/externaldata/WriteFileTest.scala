package ot.dispatcher.plugins.externaldata

import java.io.File

import ot.dispatcher.plugins.externaldata.commands.WriteFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class WriteFileTest extends CommandTest {
  override val dataset: String = """[
      |{"a":"1","b":"2"},
      |{"a":"10","b":"20"}
      |]""".stripMargin

  test("Test 0. Command: | writeFile parquet") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=parquet path=write_test_file_parquet """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("parquet").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | writeFile json") {
    val path = new File("src/test/resources/temp/write_test_file_json").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=json path=write_test_file_json """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("json").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | writeFile csv") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}

