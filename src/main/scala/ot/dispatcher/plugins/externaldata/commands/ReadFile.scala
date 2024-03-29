package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.plugins.externaldata.internals.ExternalFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

/**
 * SMaLL command. It reads any file of compatible with Spark format.
 * @param sq [[SimpleQuery]] search query object.
 * @return [[DataFrame]]
 */
class ReadFile(sq: SimpleQuery, utils: PluginUtils) extends ExternalFile(sq, utils) {

  val mergeSchema: String = getKeyword("mergeSchema").getOrElse("false")
  val inferSchema: String = getKeyword("inferSchema").getOrElse("false")

  override def transform(_df: DataFrame): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.format(format).options(
      Map("header" -> header,
        "mergeSchema" -> mergeSchema,
        "inferSchema" -> inferSchema
      )).load(absolutePath)
    df
  }

}
