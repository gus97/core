package cn.gus.core

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.reflect.internal.util.TableDef.Column


object SparkStruct4Kafka {


  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:/gus/java/hadoop_home")
    System.setProperty("HADOOP_USER_NAME", "root")

    //val fileWriter = new FileWriter(new File(s"c:/data/spark/temp"))


    val spark = SparkSession
      .builder()
      .appName("gus-1111")
      //.master("spark://172.18.111.3:7078")
      .master("local[8]")
      //.enableHiveSupport()
      .config("spark.executor.memory", "512m")
      .config("spark.driver.cores", 1)
      .config("spark.cores.max", 4)
      .config("spark.driver.memory", "512m")
      //.config("spark.driver.host", "172.16.39.52")
      .config("spark.ui.port", 4051)
      .config("spark.eventLog.dir", "hdfs://172.18.111.3:9000/spark-2.2.0/applicationHistory")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.compress", value = true)
      .config("spark.logConf", value = true)
      .getOrCreate()


    import spark.implicits._

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[Any, String]]

    val checkpointLocation = "hdfs://172.18.111.3:9000/tmp/checkpointLocation-spark001"

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.18.111.7:9092,172.18.111.8:9092,172.18.111.9:9092")
      .option("subscribe", "spark01")
//      .schema(schema)
        .load()
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]

    //==================================================================================================

    val schemaString = "eno ename"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType))
    val schema = StructType(fields)
    val rowRDD = lines
      .map(_.split(","))
      //.map(x => Row(x(0), x(1), x(2), x(3), x(4)))
    //spark.createDataFrame(rowRDD, schema).createOrReplaceTempView("foo")
    //==================================================================================================

    lines.map(_.split(",")).createOrReplaceTempView("foo")

    val wordCounts = spark.sql("SELECT * FROM foo ")


    //val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count

    /**
      * .format("parquet") \
      * .option("startingOffsets", "earliest") \
      */
    // Start running the query that prints the running counts to the console
    //var i = 0
    val query = wordCounts.writeStream.trigger(ProcessingTime(5000L))
      //complete,append,update
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreach(new ForeachWriter[Row] {

        override def process(value: Row): Unit = {

          println(value, "========>>", value.toSeq.mkString(","))

        }

        override def close(errorOrNull: Throwable): Unit = {


        }

        override def open(partitionId: Long, version: Long): Boolean = {

          true
        }
      }).start()
    query.awaitTermination()
  }
}
