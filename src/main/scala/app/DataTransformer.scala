package app

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel


object DataTransformer extends App {
    val logPrefix: String = "DataTransformer: "

    if (args.length == 1 && args(0) == "--help") {
        printHelp()
        System.exit(0)
    }

    if (args.length != 2) {
        printHelp()
        System.exit(1)
    }

    // NOTE: no checking is done for the user input
    val dateString: String = args(0)
    val inputPath: String = args(1)

    // NOTE: only checks for presence of environment variables, not their validity
    val sparkUrl: String = System.getenv("SPARK_URL")
    val outputPath: String = System.getenv("TARGET_PATH")
    val ucUrl: String = System.getenv("UC_URL")
    val ucToken: String = System.getenv("UC_TOKEN")
    val ucCatalog: String = System.getenv("UC_CATALOG")
    val ucSchema: String = System.getenv("UC_SCHEMA")
    val fullRowCount: Boolean = System.getenv("VERBOSE_ROW_COUNT") == "true"

    val missingEnvVars: List[String] = List(
        "SPARK_URL" -> sparkUrl,
        "TARGET_PATH" -> outputPath,
        "UC_URL" -> ucUrl,
        "UC_TOKEN" -> ucToken,
        "UC_CATALOG" -> ucCatalog,
        "UC_SCHEMA" -> ucSchema,
    )
        .filter({case (_, value) => value == null || value.isEmpty})
        .map({case (name, _) => name})

    if (missingEnvVars.nonEmpty) {
        println(s"${logPrefix}Error: Missing required environment variables: ${missingEnvVars.mkString(", ")}")
        printHelp()
        System.exit(1)
    }

    val spark: SparkSession = SparkSession
        .builder()
        .appName("power-quality-data-transformer")
        .config("spark.sql.catalog.unity.uri", ucUrl)
        .config("spark.sql.catalog.unity.token", ucToken)
        .config("spark.sql.defaultCatalog", ucCatalog)
        .remote(sparkUrl)
        .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)


    def printHelp(): Unit = {
        println(s"${logPrefix}Usage: DataTransformer <date-string> <input-path>")
        println(s"${logPrefix}  <date-string> : Date string in format YYYY-MM-DD")
        println(s"${logPrefix}  <input-path>  : Path to input CSV files")
        println()
        println(s"${logPrefix}The following environmental variables are required:")
        println(s"${logPrefix}- SPARK_URL : URL for Spark Connect server (e.g. 'sc://127.0.0.1:15002')")
        println(s"${logPrefix}- UC_URL: URL for Unity Catalog server (e.g. 'http://127.0.0.1:8080')")
        println(s"${logPrefix}- UC_TOKEN : Access token for Unity Catalog")
        println(s"${logPrefix}- UC_CATALOG : Catalog name for Unity Catalog")
        println(s"${logPrefix}- UC_SCHEMA : Schema name for Unity Catalog")
        println(s"${logPrefix}- UC_TABLE : Table name for Unity Catalog")
        println(s"${logPrefix}- TARGET_PATH : Path to store the transformed data (e.g. 's3://my-bucket/transformed-data/')")
        println()
        println(s"${logPrefix}The following environment variables are optional:")
        println(s"${logPrefix}- VERBOSE_ROW_COUNT : Set to 'true' to display full row counts (default: false)")
    }

    def getInputPathDevice(basePath: String, dateString: String): String = {
        s"${basePath}/${dateString}_devices.csv"
    }

    def getInputPathData(basePath: String, dateString: String): String = {
        s"${basePath}/${dateString}_data.csv"
    }

    def getInputPathEventData(basePath: String, dateString: String): String = {
        s"${basePath}/${dateString}_event_data.csv"
    }

    def getOutputPath(basePath: String, deviceId: Long): String = {
        s"${basePath}/device_${deviceId}"
    }


    def createSchemaIfNotExists(schema_name: String): Unit = {
        // Creates the schema in Unity Catalog via REST if it does not already exist.
        try {
            if (!spark.catalog.databaseExists(schema_name)) {
                println(s"${logPrefix}Creating schema ${schema_name} in Unity Catalog")
                val schemaUrl: String = HttpUtils.getSchemaCreationUrl(ucUrl)
                val headers: Map[String, String] = HttpUtils.getQueryHeaders(ucToken)
                val payload: String = HttpUtils.getSchemaPayload(ucCatalog, ucSchema)
                val response = HttpUtils.makeRequest(HttpUtils.postRequest(schemaUrl, headers, payload))

                if (!response.code.isSuccess && response.code.code != 409) {
                    throw new RuntimeException(
                        s"Schema creation failed with status ${response.code.code}: ${response.body}"
                    )
                }
            }
        } catch {
            case _: Throwable =>
        }
    }

    def createTableToCatalog(data: DataFrame, table_name: String, targetPath: String): Unit = {
        // Creates the table in Unity Catalog based on the columns in the input data frame.
        println(s"${logPrefix}Creating table ${table_name} in Unity Catalog")
        val nameParts = table_name.split("\\.", 3)
        val (catalogName, schemaName, tableName) =
            if (nameParts.length == 3) {
                (nameParts(0), nameParts(1), nameParts(2))
            } else {
                (ucCatalog, ucSchema, table_name)
            }

        val tableUrl: String = HttpUtils.getTableCreationUrl(ucUrl)
        val headers: Map[String, String] = HttpUtils.getQueryHeaders(ucToken)
        val payload: String = HttpUtils.getTablePayload(data, catalogName, schemaName, tableName, targetPath)
        val response = HttpUtils.makeRequest(HttpUtils.postRequest(tableUrl, headers, payload))

        if (!response.code.isSuccess && response.code.code != 409) {
            throw new RuntimeException(
                s"Table creation failed with status ${response.code.code}: ${response.body}"
            )
        }
    }


    def storeNewData(data: DataFrame, targetPath: String): Unit = {
        // Stores the new data in the target path as a Delta table.
        data
            .write
            .format("delta")
            .mode("overwrite")
            .option("path", targetPath)
            .save()
    }

    def addData(data: DataFrame, targetPath: String): Unit = {
        // Adds data to an existing Delta table, avoiding duplicates based on device_id, timestamp, and event_id.
        val existingData = spark
            .read
            .format("delta")
            .load(targetPath)

        val rowsToInsert = data.alias("new")
            .join(
                existingData.alias("orig"),
                col("orig.device_id") === col("new.device_id") &&
                col("orig.timestamp") === col("new.timestamp") &&
                (
                    col("orig.event_id") === col("new.event_id") ||
                    (col("orig.event_id").isNull && col("new.event_id").isNull)
                ),
                "left_anti"
            )

        rowsToInsert
            .write
            .format("delta")
            .mode("append")
            .save(targetPath)
    }


    def storeDeviceData(deviceId: Long, df: DataFrame, targetPath: String): Unit = {
        val deviceData = df
            .filter(col("device_id") === deviceId)
            .orderBy("timestamp", "event_id")
        val firstRow: Option[Row] = deviceData.head(1).headOption

        if (firstRow.isEmpty) {
            println(s"${logPrefix}- 0 data rows stored for device ${deviceId}")
            return
        }

        val targetFolder: String = getOutputPath(targetPath, deviceId)
        val fullTableName: String = s"${ucCatalog}.${ucSchema}.data_device_${deviceId}"

        val oldDataExists: Boolean = try {
            spark.read.format("delta").load(targetFolder).limit(1).count() == 1
        } catch {
            case _: Throwable => false
        }

        val tableExists: Boolean = try {
            spark.catalog.tableExists(fullTableName)
        } catch {
            case _: Throwable => false
        }

        // If the table does not exists, create it
        if (!tableExists) {
            createTableToCatalog(deviceData, fullTableName, targetFolder)
        }

        // If the Delta table exists, add new data to it, avoiding duplicates
        if (oldDataExists) {
            addData(deviceData, targetFolder)
        }
        else {
            storeNewData(deviceData, targetFolder)
        }


        println(s"${logPrefix}- ${deviceData.count()} data rows stored for device ${deviceId}")
        if (fullRowCount) {
            val fullCount = spark.read.format("delta").load(targetFolder).count()
            println(s"${logPrefix}  - ${fullCount} data rows in total for device ${deviceId}")
        }
    }


    val devicePath: String = getInputPathDevice(inputPath, dateString)
    val dataPath: String = getInputPathData(inputPath, dateString)
    val eventDataPath: String = getInputPathEventData(inputPath, dateString)


    // Load device list and data
    val devices: List[Long] = Fetchers.getDevices(spark, devicePath)
    val eventDataExists: Boolean = Fetchers.checkEventData(spark, eventDataPath)
    val main_data_df: DataFrame = Fetchers.getData(spark, dataPath)


    // Combine main data with event data if it exists
    val full_data_df: DataFrame = (eventDataExists match {
        case true =>
            main_data_df
                .union(Fetchers.getEventData(spark, eventDataPath))
        case false =>
            main_data_df
    })
        .persist(StorageLevel.MEMORY_ONLY)


    // Store data for each device
    println(s"${logPrefix}Storing data from ${devices.length} devices for date ${dateString}")
    createSchemaIfNotExists(s"${ucCatalog}.${ucSchema}")
    devices
        .foreach(deviceId => storeDeviceData(deviceId, full_data_df, outputPath))


    spark.stop()
}
