package cn.gus.core


import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{ForeachWriter, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import java.util

import org.apache.spark.sql.catalyst.encoders.RowEncoder

object SparkStruct4Kafka {

  case class Tracklog(dateday: String, datetime: String, ip: String)


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

    //implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    //        val schema = StructType(
    //          StructField("eno", StringType, nullable = false) ::
    //            StructField("name", StringType, nullable = false) ::
    //            StructField("age", StringType, nullable = false) ::
    //            StructField("sal", StringType, nullable = false) ::
    //            StructField("dno", StringType, nullable = false) ::Nil)


    val checkpointLocation = "hdfs://172.18.111.3:9000/tmp/checkpointLocation-spark0077"

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.18.111.7:9092,172.18.111.8:9092,172.18.111.9:9092")
      .option("subscribe", "spark01")
      //      .schema(schema)
      .load()
      .selectExpr("CAST(value AS STRING) as id")
      .as[(String)]

    //==================================================================================================



    val schemaString = "a,b,c"

    // Generate the schema based on the string of schema
    val fields = new util.ArrayList[StructField]
    for (fieldName <- schemaString.split(",")) {
      val field = DataTypes.createStructField(fieldName, DataTypes.StringType, true)
      fields.add(field)
    }
    val schema = DataTypes.createStructType(fields)


    //    var emp = spark.createDataFrame(rowRDD.rdd, schema)
    //    emp.createOrReplaceTempView("emp")
    //    val wordCounts = spark.sql("SELECT dno,sum(sal) as s_m FROM emp group by dno")
    //==================================================================================================

    //lines.map(_.split(",")).map(x => Tracklog(x(0), x(1), x(2))).toDF("a", "b", "c").createOrReplaceTempView("tb3")
    lines.map(_.split(",")).filter(_.length==3).map(x => RowFactory.create(x(0), x(1), x(2)))(RowEncoder.apply(schema)).createOrReplaceTempView("foo")

    val wordCounts = spark.sql("SELECT c,sum(b+a) FROM foo group by c ")


    //val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count

    /**
      * .format("parquet") \
      * .option("startingOffsets", "earliest") \
      */
    // Start running the query that prints the running counts to the console
    var i = 0
    val query = wordCounts.writeStream.trigger(ProcessingTime(5000L))
      //complete,append,update
      .outputMode("update")
      .option("checkpointLocation", checkpointLocation)
      .foreach(new ForeachWriter[Row] {

        override def process(value: Row): Unit = {

          println(value.toSeq.mkString(","))



        }

        override def close(errorOrNull: Throwable): Unit = {

          //println(Thread.currentThread().getId,"-------------->>",i)
        }

        override def open(partitionId: Long, version: Long): Boolean = {

          //i += 1
          //println(Thread.currentThread().getId,"================>>",i)
          true
        }
      }).start()
    query.awaitTermination()
  }
}
