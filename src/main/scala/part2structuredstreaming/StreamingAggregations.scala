package part2structuredstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, split, sum}
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

    val pokemonDF= lines
      .withColumn("Number",split(col("value"),",").getItem(0))
      .withColumn("Name",split(col("value"),",").getItem(1))
      .withColumn("Type_1",split(col("value"),",").getItem(2))
      .withColumn("Type_2",split(col("value"),",").getItem(3))
      .withColumn("Total",split(col("value"),",").getItem(4))
      .withColumn("HP",split(col("value"),",").getItem(5))
      .withColumn("Attack",split(col("value"),",").getItem(6))
      .withColumn("Defense",split(col("value"),",").getItem(7))
      .withColumn("Sp_Atk",split(col("value"),",").getItem(8))
      .withColumn("Sp_Def",split(col("value"),",").getItem(9))
      .withColumn("Speed",split(col("value"),",").getItem(10))
      .withColumn("Generation",split(col("value"),",").getItem(11))
      .withColumn("Legendary",split(col("value"),",").getItem(12))
      .filter(col("Name") =!= lit("Name"))

    pokemonDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    pokemonSocket()
  }
}
