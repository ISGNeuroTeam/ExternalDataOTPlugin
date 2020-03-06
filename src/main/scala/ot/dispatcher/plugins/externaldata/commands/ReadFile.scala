package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.plugins.externaldata.internals.ExternalFile
import ot.scalaspl.SimpleQuery

/**
 * SMaLL command. It reads any file of compatible with Spark format.
 * @param sq [[SimpleQuery]] search query object.
 * @return [[DataFrame]]
 */
class ReadFile(sq: SimpleQuery) extends ExternalFile(sq) {

  override def transform(_df: DataFrame): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.format(format).load(absolutePath)
    df
  }

}
