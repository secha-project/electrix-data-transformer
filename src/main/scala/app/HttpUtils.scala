package app

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{
    BinaryType, BooleanType, ByteType, DataType, DateType, DoubleType, FloatType,
    IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType
}
import sttp.client4.{Request, Response}
import sttp.client4.quick.{quickRequest, RichRequest, UriContext}


object HttpUtils {
    // implicit val ec: ExecutionContext = ExecutionContext.global

    private def escapeJson(value: String): String = {
        value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\b", "\\b")
            .replace("\f", "\\f")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
    }

    val sparkToUcTypeMap: Map[DataType, String] = Map(
        BooleanType -> "BOOLEAN",
        ByteType -> "BYTE",
        ShortType -> "SHORT",
        IntegerType -> "INT",
        LongType -> "LONG",
        FloatType -> "FLOAT",
        DoubleType -> "DOUBLE",
        DateType -> "DATE",
        TimestampType -> "TIMESTAMP",
        TimestampNTZType -> "TIMESTAMP_NTZ",
        StringType -> "STRING",
        BinaryType -> "BINARY",
    )

    def getColumnInformationString(df: DataFrame): String = {
        df.schema.fields
            .zipWithIndex
            .map({
                case (field, index) =>
                    Seq(
                        s"\"name\":\"${escapeJson(field.name)}\"",
                        s"\"type_name\":\"${escapeJson(sparkToUcTypeMap.getOrElse(field.dataType, "UNKNOWN"))}\"",
                        s"\"type_text\":\"${escapeJson(field.dataType.catalogString)}\"",
                        s"\"type_json\":\"${escapeJson(field.dataType.json)}\"",
                        s"\"nullable\":${field.nullable}",
                        s"\"position\":${index}",
                        "\"partition_index\":null",
                    ).mkString("{", ",", "}")
            })
            .mkString("[", ",", "]")
    }

    def getTablePayload(df: DataFrame, catalog: String, schema: String, table: String, targetPath: String): String = {
        Seq(
            s"\"name\":\"${escapeJson(table)}\"",
            s"\"catalog_name\":\"${escapeJson(catalog)}\"",
            s"\"schema_name\":\"${escapeJson(schema)}\"",
            s"\"table_type\":\"EXTERNAL\"",
            s"\"data_source_format\":\"DELTA\"",
            s"\"storage_location\":\"${escapeJson(targetPath)}\"",
            s"\"columns\":${getColumnInformationString(df)}",
            s"\"comment\":\"Registered via Spark+REST to keep UC columns in sync\""
        )
            .mkString("{", ",", "}")
    }

    def getQueryHeaders(token: String): Map[String, String] = {
        Map(
            "Authorization" -> s"Bearer ${token}",
            "Content-Type" -> "application/json"
        )
    }

    def getTableCreationUrl(unityBaseUrl: String): String = {
        s"${unityBaseUrl}/api/2.1/unity-catalog/tables"
    }

    def getSchemaCreationUrl(unityBaseUrl: String): String = {
        s"${unityBaseUrl}/api/2.1/unity-catalog/schemas"
    }

    def getSchemaPayload(catalog: String, schema: String): String = {
        Seq(
            s"\"name\":\"${escapeJson(schema)}\"",
            s"\"catalog_name\":\"${escapeJson(catalog)}\""
        )
            .mkString("{", ",", "}")
    }

    def postRequest(targetUrl: String, headers: Map[String, String], payload: String): Request[String] = {
        quickRequest
            .post(uri"$targetUrl")
            .headers(headers)
            .body(payload)
    }

    def makeRequest(request: Request[String]): Response[String] = {
        request.send()
    }

    def printResponse(response: Response[String]): Unit = {
        println(s"Response code: ${response.code}")
        println(s"Response body: ${response.body}")
    }
}
