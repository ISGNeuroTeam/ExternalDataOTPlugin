package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.{DataFrame, SaveMode}
import ot.dispatcher.plugins.externaldata.internals.ExternalFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

/**
 * SMaLL command. It writes any file of compatible with Spark format.
 *
 * Example: | writeFile format=parquet partitionBy=col1,col2 mode=append
 *
 * @param sq [[SimpleQuery]] search query object.
 * @return [[DataFrame]]
 */
class WriteFile(sq: SimpleQuery, utils: PluginUtils) extends ExternalFile(sq, utils) {

  private val mode = getKeyword("mode") match {
    case Some("append") => SaveMode.Append
    case Some("overwrite") => SaveMode.Overwrite
    case Some(_) => sendError("Specified save mode is not supported")
    case _ => SaveMode.Overwrite
  }

  private val partitionBy = getKeyword("partitionBy").map(_.split(",").map(_.trim))

  override def transform(_df: DataFrame): DataFrame = {
    val dfw = _df.write.format(format).mode(mode).option("header", "true")
    partitionBy match {
      case Some(partitions) if partitions.forall(_df.columns.contains) => dfw.partitionBy(partitions: _*).save(absolutePath)
      case Some(partitions) => {
        val missedCols = partitions.filterNot(_df.columns.contains)
        sendError(s"Missed columns: '${missedCols.mkString(", ")}")
      }
      case _ => dfw.save(absolutePath)
    }
    _df
  }
}
