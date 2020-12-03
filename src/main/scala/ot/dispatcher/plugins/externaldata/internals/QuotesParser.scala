package ot.dispatcher.plugins.externaldata.internals

import java.io.File

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.{Keyword, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

/** Parses key=value pairs from command args.
 * Supports quotes.
 */
abstract class QuotesParser(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  override def keywordsParser= (args: String) => {
    """([^'"\s]*)\s*=((?:(?:'|")[^'"]*(?:'|"))|(?:\s*(?:[^'"=\s]+)))""".r.findAllIn(args)
      .matchData
      .map(x =>
        Keyword(
          key = x.group(1),
          value = x.group(2)
        )
      )
      .toList
  }



}
