import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

/**
  * Created by jie.sun on 2018/8/15.
  */

object CloudTrailETL {

  //0,SparkSession
  val spark = SparkSession
    .builder()
    .appName("CloudTrailETL")
    .getOrCreate()

  //1,input and output path
  val cloudTrailLogsPath = ""
  val parquetOutputPath = ""

  //2,schema of data
  val cloudTrailSchema = new StructType()
    .add("Records", ArrayType(new StructType()
      .add("additionalEventData",StringType)
      .add("apiVersion", StringType)
      .add("awsRegion", StringType)
      .add("errorCode", StringType)
      .add("errorMessage", StringType)
      .add("eventID", StringType)
      .add("eventName", StringType)
      .add("eventSource", StringType)
      .add("eventTime", StringType)
      .add("eventType", StringType)
      .add("eventVersion", StringType)
      .add("readOnly", BooleanType)
      .add("recipientAccountId", StringType)
      .add("requestID", StringType)
      .add("requestParameters", MapType(StringType, StringType))
      .add("resources", ArrayType(new StructType()
        .add("ARN", StringType)
        .add("accountId", StringType)
        .add("type", StringType)
      ))
      .add("responseElements", MapType(StringType, StringType))
      .add("sharedEventID", StringType)
      .add("sourceIPAddress", StringType)
      .add("serviceEventDetails", MapType(StringType, StringType))
      .add("userAgent", StringType)
      .add("userIdentity", new StructType()
        .add("accessKeyId", StringType)
        .add("accountId", StringType)
        .add("arn", StringType)
        .add("invokedBy", StringType)
        .add("principalId", StringType)
        .add("sessionContext", new StructType()
          .add("attributes", new StructType()
            .add("creationDate", StringType)
            .add("mfaAuthenticated", StringType)
          )
          .add("sessionIssuer", new StructType()
            .add("accountId", StringType)
            .add("arn", StringType)
            .add("principalId", StringType)
            .add("type", StringType)
            .add("userName", StringType)
          )
        )
        .add("type", StringType)
        .add("userName", StringType)
        .add("webIdFederationData", new StructType()
          .add("federatedProvider", StringType)
          .add("attributes", MapType(StringType, StringType))
        )
      )
      .add("vpcEndpointId", StringType)
    ))

  //3,do streaming ETL on it
  val rawRecords = spark.readStream
    .option("maxFilesPerTrigger", 100)
    .schema(cloudTrailSchema)
    .json(cloudTrailLogsPath)

  val cloudTrailEvents = rawRecords
    .select(explode(rawRecords("Recordes")) as "record")
    .select(
      unix_timestamp(rawRecords("record.eventTime"),"yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "timestamp",
      rawRecords("record.*"))

  //define the checkpoint location
  val checkpointPath = "/checkpoint/path"

  val streamingETLQuery = cloudTrailEvents.withColumn("date",cloudTrailEvents("timestamp").cast("date"))
    .writeStream
    .format("parquet")
    .option("path", parquetOutputPath)
    .partitionBy("date")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation",checkpointPath)
    .start()

  import spark.sql
  val parquetData = sql(s"select * from parquet.`$parquetOutputPath`")

  val count = sql(s"select * from parquet.`$parquetOutputPath`").count()

}
