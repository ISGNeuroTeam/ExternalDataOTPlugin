package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.{DataFrame, SaveMode}
import ot.dispatcher.plugins.externaldata.internals.ExternalFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

/**
 * SMaLL command. It writes any file of compatible with Spark format.
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

  private val partitionBy = getKeyword("partition")

  override def transform(_df: DataFrame): DataFrame = {
    val dfw = _df.write.format(format).mode(mode).option("header", "true")
    partitionBy match {
      case Some(partition) if _df.columns.contains(partition) => dfw.partitionBy(partition).save(absolutePath)
      case Some(partition) => sendError(s"Dataframe does not contain the '$partition' column")
      case _ => dfw.save(absolutePath)
    }
    _df
  }
}
