package app

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel


object DataTransformer extends App {
    val logPrefix: String = "DataTransformer: "

    if (args.length == 1 && args(0) == "--help") {
        printHelp()
        System.exit(0)
    }

    if (args.length != 3) {
        printHelp()
        System.exit(1)
    }

    // No argument validation is done here!
    val dateString: String = args(0)
    val inputPath: String = args(1)
    val outputPath: String = args(2)

    val checkTable: Boolean = System.getenv("CHECK_DELTA_TABLE") == "true"
    val fullRowCount: Boolean = System.getenv("VERBOSE_ROW_COUNT") == "true"


    val spark : SparkSession= SparkSession
        .builder()
        .appName("power-quality-data-transformer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local")
        .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

    val schemaName = "secha"
    spark.sql(s"CREATE SCHEMA IF NOT EXISTS ${schemaName}")


    def printHelp(): Unit = {
        println(s"${logPrefix}Usage: DataTransformer <date-string> <input-path> <output-path>")
        println(s"${logPrefix}  <date-string> : Date string in format YYYY-MM-DD")
        println(s"${logPrefix}  <input-path>  : Path to input CSV files")
        println(s"${logPrefix}  <output-path> : Path to output Delta Lake tables")
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

        val tableName = s"${schemaName}.device_${deviceId}"
        val tableExists: Boolean = spark.catalog.tableExists(tableName)
        val oldDataExists: Boolean = try {
            DeltaTable.isDeltaTable(spark, targetFolder)
        } catch {
            case _: Throwable => false
        }

        // If the table does not exist, create it with the new data
        if (!oldDataExists || (checkTable && !tableExists)) {
            deviceData
                .write
                .format("delta")
                .mode("overwrite")
                .option("path", targetFolder)
                .saveAsTable(tableName)
        }

        // Load the data from the target folder as a Delta table
        val deltaTable = DeltaTable.forPath(spark, targetFolder)

        // Update old data with new data, avoiding duplicates
        if (oldDataExists && (!checkTable || tableExists)) {
            deltaTable
                .as("orig")
                .merge(
                    deviceData.alias("new"),
                    col("orig.device_id") === col("new.device_id") &&
                    col("orig.timestamp") === col("new.timestamp") &&
                    (
                        col("orig.event_id") === col("new.event_id") ||
                        (col("orig.event_id").isNull && col("new.event_id").isNull)
                    )
                )
                .whenNotMatched()
                    .insertAll()
                .execute()
        }

        println(s"${logPrefix}- ${deviceData.count()} data rows stored for device ${deviceId}")
        if (fullRowCount) {
            println(s"${logPrefix}  - ${deltaTable.toDF.count()} data rows in total for device ${deviceId}")
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
    devices
        .foreach(deviceId => storeDeviceData(deviceId, full_data_df, outputPath))


    spark.stop()
}
