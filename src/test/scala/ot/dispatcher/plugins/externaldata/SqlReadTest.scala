package ot.dispatcher.plugins.externaldata

import org.apache.spark.sql.SparkSession
import org.scalatest.Ignore
import ot.dispatcher.plugins.externaldata.commands.SQLRead
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

@Ignore
class SqlReadTest extends CommandTest {
  override val dataset: String =
    """
      |+-----+
      ||field|
      |+-----+
      ||    1|
      |+-----+
      |""".stripMargin

//  override val spark: SparkSession = SparkSession.builder()
//    .appName("Small plugin Tests")
//    .master("local")
//    .config("spark.sql.files.ignoreCorruptFiles", value = true)
//    // It doesnt work. I've no time to discovery why.
//    // .config("spark.driver.extraClassPath", "/home/andrey/SpeedDir/projects/IdeaProjects/ExternalDataOTPlugin/src/test/resources/postgresql-42.2.5.jar")
//    .getOrCreate()


  test("Read from Postgresql DB.") {
    val simpleQuery = SimpleQuery(""" base=postgres host=localhost user=dispatcher password=P@$$w0rd db=db query="SELECT 1 as field" """)
    val commandSQLRead = new SQLRead(simpleQuery, utils)
    val actual = execute(commandSQLRead)
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
