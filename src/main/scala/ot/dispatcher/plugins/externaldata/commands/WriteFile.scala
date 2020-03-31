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

  override def transform(_df: DataFrame): DataFrame = {
    _df.write.format(format).mode(SaveMode.Overwrite).save(absolutePath)
    _df
  }

}
