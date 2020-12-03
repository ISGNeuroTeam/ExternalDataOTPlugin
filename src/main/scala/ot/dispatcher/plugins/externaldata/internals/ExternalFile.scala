package ot.dispatcher.plugins.externaldata.internals

import java.io.File

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

/** Provides arguments from SMaLL command for ReadFile and WriteFile.
 * @param sq [[SimpleQuery]] search query object.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class ExternalFile(sq: SimpleQuery, utils: PluginUtils) extends QuotesParser(sq, utils) {

  val format: String = getKeyword("format").get
  val path: String = getKeyword("path").get.replace("../","")
  val fs: String = pluginConfig.getString("storage.fs")
  val basePath: String = pluginConfig.getString("storage.path")
  val absolutePath: String = fs + new File(basePath,path).getAbsolutePath
  log.info(s"Absolute path: $absolutePath. Format: $format")

  val requiredKeywords: Set[String] = Set("format", "path")
  val optionalKeywords: Set[String] = Set()

  override def transform(_df: DataFrame): DataFrame = _df
}
