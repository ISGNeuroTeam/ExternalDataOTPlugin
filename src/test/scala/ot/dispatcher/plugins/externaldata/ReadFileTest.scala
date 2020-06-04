package ot.dispatcher.plugins.externaldata

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.externaldata.commands.ReadFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class ReadFileTest extends CommandTest {
  override val dataset: String = """[
      |{"a":"1","b":"2"},
      |{"a":"10","b":"20"}
      |]""".stripMargin

  val initialDf: DataFrame = jsonToDf(dataset)

  test("Test 0. Command: | readFile parquet") {
    initialDf.show()
    val path = new File("src/test/resources/temp/read_test_file_parquet").getAbsolutePath
    initialDf.write.format("parquet").save(path)
    val simpleQuery = SimpleQuery(""" format=parquet path=read_test_file_parquet """)
    val commandReadFile = new ReadFile(simpleQuery, utils)
    val actual = execute(commandReadFile)
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | readFile json") {
    initialDf.show()
    val path = new File("src/test/resources/temp/read_test_file_json").getAbsolutePath
    initialDf.write.format("json").save(path)
    val simpleQuery = SimpleQuery(""" format=json path=read_test_file_json """)
    val commandReadFile = new ReadFile(simpleQuery, utils)
    val actual = execute(commandReadFile)
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | readFile csv") {
    initialDf.show()
    val path = new File("src/test/resources/temp/read_test_file_csv").getAbsolutePath
    initialDf.write.format("csv").option("header", "true").save(path)
    val simpleQuery = SimpleQuery(""" format=csv path=read_test_file_csv """)
    val commandReadFile = new ReadFile(simpleQuery, utils)
    val actual = execute(commandReadFile)
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}

