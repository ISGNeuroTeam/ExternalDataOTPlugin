# OT Platform. External Data plugin.

Additional commands for reading/writing in **Dispatcher App**.

## Getting Started

You need published local dispatcher-sdk lib in order not to use unmanaged libs. 

### Command List
#### readFile - reads any compatible with Spark file format.
    | readFile format=__format__ path=__path__
option("header", "true") is hardcoded.

#### writeFile - writes any compatible with Spark file format.
    | writeFile format=__format__ path=__path__
option("header", "true") is hardcoded.

#### sqlRead - gets table from SQL database.
    | sqlread base=__base__ host=__host__ user=__user__ password=__password__ [db=__database__] [table=__table__] [query=__"query"__] [numPartitions=__numPartitions__] [partitionColumn=__partitionColumn__]  [lowerBound=__lowerBound__] [upperBound=__upperBound__] [fetchSize=__fetchSize__]
Compatible bases: Oracle, Microsoft SQL, PostgreSQL.

Arguments:
 - base - database type (oracle, mssql, postgres).
 - host - host name of SQL server.
 - user - connection username.
 - password - connection password.
 - db - database name if exists (for MS and Postgres).
 - table -  the JDBC table that should be read from. Note that when using it in the read path anything
that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could
also use a subquery in parentheses. It is not allowed to specify `table` and `query` options at the same time.
 - query -  a query that will be used to read data into Spark. Below are a couple of restrictions while using this option.
It is not allowed to specify `table` and `query` options at the same time.
It is not allowed to specify `query` and `partitionColumn` options at the same time.
When specifying `partitionColumn` option is required, the subquery can be specified using `table` option
instead and partition columns can be qualified using the subquery alias provided as part of `table`.
 - numPartitions - the maximum number of partitions that can be used for parallelism in table reading. This also
determines the maximum number of concurrent JDBC connections.
 - partitionColumn, lowerBound, upperBound - these options must all be specified if any of them is specified.
In addition, numPartitions must be specified. They describe how to partition the table when reading in parallel from multiple workers.
`partitionColumn` must be a numeric, date, or timestamp column from the table in question.
Notice that lowerBound and upperBound are just used to  decide the partition stride, not for filtering the rows in table.
So all rows in the table will be partitioned and returned. 
 - fetchSize - the JDBC fetch size, which determines how many rows to fetch per round trip.
This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).
The default value - 100000 rows.

## Running the tests
 
 sbt test

## Deploying

1. make pack or make build.
2. Copy the `build/ExternalDataOTPlugin` directory to `/opt/otp/dispatcher/plugins` (or unpack the resulting archive `externaldataotplugin-<PluginVersion>-<Branch>.tar.gz` into the same directory).
3. Rename loglevel.properties.example => loglevel.properties.
4. Rename plugin.conf.example => plugin.conf.
5. If necessary, configure plugin.conf and loglevel.properties.
6. Restart dispatcher.

## Dependencies

- dispatcher-sdk_2.11  1.2.0
- sbt 1.5.8
- scala 2.11.12
- eclipse temurin 1.8.0_312 (formerly known as AdoptOpenJDK)

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository. 

## Authors
 
Andrey Starchenkov (astarchenkov@ot.ru)  

## License

[OT.PLATFORM. License agreement.](LICENSE.md)
