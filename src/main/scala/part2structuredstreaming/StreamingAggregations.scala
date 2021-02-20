package part2structuredstreaming

import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  val pokemonSchema: StructType = StructType(Array(
    StructField("Number",StringType),
    StructField("Name",StringType),
    StructField("Type_1",StringType),
    StructField("Type_2",StringType),
    StructField("Total",StringType),
    StructField("HP",LongType),
    StructField("Attack",LongType),
    StructField("Defense",LongType),
    StructField("Sp_Atk",LongType),
    StructField("Sp_Def",LongType),
    StructField("Speed",LongType),
    StructField("Generation",LongType),
    StructField("Legendary",StringType)
  ))

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def pokemonSocket(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    case class pokemon(number:String,name:String,type_1:String,type_2:String,total:Long,hp:Long,attack:Long,defense:Long,sp_atk:Long,sp_def:Long,speed:Long,generation:Int,legendary:String)

    val pokemonRdd = lines.rdd
      .map(line => line.toString().split(","))
      .map(item => Row(pokemon(item(0),item(1),item(2),item(3),item(4).toLong,item(5).toLong,item(6).toLong,item(7).toLong,item(8).toLong,item(9).toLong,item(10).toLong,item(11).toInt,item(12))))

    val pokemonDF = spark.createDataFrame(pokemonRdd,pokemonSchema)

    pokemonDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    pokemonSocket()
  }
}
