package ot.dispatcher.plugins.externaldata.internals

import org.apache.spark.sql.DataFrame
import ot.scalaspl.SimpleQuery
import ot.dispatcher.sdk.PluginCommand

/** Provides arguments from SMaLL command for ReadFile and WriteFile.
 * @param sq [[SimpleQuery]] search query object.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class ExternalFile(sq: SimpleQuery) extends PluginCommand(sq) {

  val format: String = getKeyword("format").get
  val path: String = getKeyword("path").get.replace("../","")
  val fs: String = pluginConfig.getString("storage.fs")
  val basePath: String = pluginConfig.getString("storage.path")
  val absolutePath: String = fs + basePath + path
  log.info(s"Absolute path: $absolutePath. Format: $format")

  override val requiredKeywords: Set[String] = Set("format", "path")
  override val optionalKeywords: Set[String] = Set()

  override def transform(_df: DataFrame): DataFrame = _df
}
