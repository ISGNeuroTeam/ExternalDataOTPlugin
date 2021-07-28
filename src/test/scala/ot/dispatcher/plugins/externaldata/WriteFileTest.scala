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

  val datasetToAppend: String = """[
       |{"a":"100","b":"200"}
       |]""".stripMargin

  val dataset3cols: String = """[
       |{"a":"1","b":"2","c":"3"},
       |{"a":"10","b":"2","c":"30"},
       |{"a":"10","b":"20","c":"300"}
       |]""".stripMargin

  val appended: String = """[
       {"a":"1","b":"2"},
       |{"a":"10","b":"20"},
       |{"a":"100","b":"200"}
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

  test("Test 3. Command: | writeFile parquet + partitionBy") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" partitionBy=a path=write_test_file_parquet """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val expected = jsonToDf(dataset)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b").sort("a")
    assert(actualDF.rdd.getNumPartitions == 2)
    assert(actualDF.except(expected).count() == 0)
  }

  test("Test 4. Command: | writeFile parquet + partitionBy on multiple columns") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" partitionBy=a,b path=write_test_file_parquet """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(jsonToDf(dataset3cols), commandWriteFile)
    val expected = jsonToDf(dataset3cols)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b", "c").sort("a")
    assert(actualDF.rdd.getNumPartitions == 3)
    assert(actualDF.except(expected).count() == 0)
  }

  test("Test 5. Command: | writeFile modes") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath

    execute(jsonToDf(datasetToAppend), new WriteFile(SimpleQuery(""" path=write_test_file_parquet """), utils))
    execute(jsonToDf(dataset), new WriteFile(SimpleQuery(""" path=write_test_file_parquet """), utils))
    execute(jsonToDf(datasetToAppend), new WriteFile(SimpleQuery(""" path=write_test_file_parquet mode=append """), utils))

    val expected = jsonToDf(appended)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b").sort("a")

    assert(actualDF.except(expected).count() == 0)
  }
}

