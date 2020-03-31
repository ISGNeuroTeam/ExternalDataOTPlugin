package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

/**
  * Gets table from SQL database.
  * Compatible bases: Oracle, Microsoft SQL, PostgreSQL.
  *
  * ==SMaLL Syntax==
  * | sqlread base=__base__ host=__hsot__ user=__user__ password=__password__ db=__database__ table=__table__
  * | sqlread base=__base__ host=__hsot__ user=__user__ password=__password__ db=__database__ query=__query__
  * | sqlread base=__base__ host=__hsot__ user=__user__ password=__password__ db=__database__ table=__table__ partitionColumn=id numPartitions=10
  * | sqlread base=__base__ host=__hsot__ user=__user__ password=__password__ db=__database__ table=__table__ partitionColumn=id numPartitions=10 lowerBound=1000 upperBound=2000
  * | sqlread base=__base__ host=__hsot__ user=__user__ password=__password__ db=__database__ table=__table__ fetchSize=__fetchSize__
  *
  * ==SMaLL args==
  * base - Database type (oracle, mssql, postgres).
  * host - Host name of SQL server.
  * user - Connection user name.
  * password - Connection password.
  * db - Database name if exists (for MS and Postgres).
  * table -  The JDBC table that should be read from or written into. Note that when using it in the read path anything
  * that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could
  * also use a subquery in parentheses. It is not allowed to specify `table` and `query` options at the same time.
  *
  * query -  A query that will be used to read data into Spark. Below are couple of restrictions while using this option.
  * It is not allowed to specify `dbtable` and `query` options at the same time.
  * It is not allowed to specify `query` and `partitionColumn` options at the same time.
  * When specifying `partitionColumn` option is required, the subquery can be specified using `table` option
  * instead and partition columns can be qualified using the subquery alias provided as part of `table`.
  *
  * numPartitions - The maximum number of partitions that can be used for parallelism in table reading. This also
  * determines the maximum number of concurrent JDBC connections.
  *
  * partitionColumn, lowerBound, upperBound - These options must all be specified if any of them is specified.
  * In addition, numPartitions must be specified. They describe how to
  * partition the table when reading in parallel from multiple workers.
  * partitionColumn must be a numeric, date, or timestamp column from the
  * table in question. Notice that lowerBound and upperBound are just used to
  * decide the partition stride, not for filtering the rows in table.
  * So all rows in the table will be partitioned and returned.
  *
  * fetchSize - The JDBC fetch size, which determines how many rows to fetch per round trip.
  * This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).
  *
  * @param sq SimpleQuery object with search information.
  */
class SQLRead(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  import utils._

  val requiredKeywords: Set[String] = Set("base", "host", "user", "password")
  val optionalKeywords: Set[String] = Set(
    "fetchSize", "db", "query", "table",
    "partitionColumn", "lowerBound", "upperBound", "numPartitions"
  )

  override def transform(_df: DataFrame): DataFrame = {

    val base = getKeyword("base").get
    val host = getKeyword("host").get
    val user = getKeyword("user").get
    val password = getKeyword("password").get

    val fetchSize = getKeyword("fetchSize").getOrElse("100000").toInt

    val dbName = getKeyword("db")
    val query = getKeyword("query")
    val dbTable = getKeyword("table")

    val dbname = dbName match {
      case Some(name) => name
      case None => None
    }

    val (url: String, driver: String) = base match {
      case "postgres" =>
        val url = s"jdbc:postgresql://$host/$dbname"
        val driver = "org.postgresql.Driver"
        (url, driver)
      case "mssql" =>
        val url = s"jdbc:sqlserver://$host;databaseName=$dbname"
        val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        (url, driver)
      case "oracle" =>
        val url = s"jdbc:oracle:thin:@$host:1521:XE"
        val driver = "oracle.jdbc.OracleDriver"
        (url, driver)
      case _ => sendError("Unknown type of database.")
    }

    val table = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", password)
      .option("fetchsize", fetchSize)

    (query, dbTable) match {
      case (Some(q), _) =>
        val query_without_edge_quotes = "^\"|\"$".r.replaceAllIn(q, "")
        table.option("query", query_without_edge_quotes)
      case (None, Some(t)) =>
        table.option("dbtable", t)
        val partitionColumn = getKeyword("partitionColumn")
        val numPartitions = getKeyword("numPartitions")
        (partitionColumn, numPartitions) match {
          case (Some(c), Some(n)) =>
            val lowerBound = getKeyword("lowerBound").getOrElse(Long.MinValue.toString).toLong
            val upperBound = getKeyword("upperBound").getOrElse(Long.MaxValue.toString).toLong
            table.option("partitionColumn", c)
            table.option("numPartitions", n)
            table.option("lowerBound", lowerBound)
            table.option("upperBound", upperBound)
          case _ =>
        }
      case (_, _) => sendError("Query and Table are empty.")
    }


    table.load()

  }

}

