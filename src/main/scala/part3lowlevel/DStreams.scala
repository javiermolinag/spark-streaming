package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._
import org.apache.spark.rdd.RDD

import java.util.Locale
import scala.collection.mutable

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark Streaming Context = entry point to the DStreams API
    - needs the spark context
    - a duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams
    - call an action on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination, or stop the computation
      - you cannot restart the ssc
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    // wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /*
      ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy", Locale.ENGLISH)

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  val rdd1: RDD[String] = spark.sparkContext.parallelize(Seq((System.currentTimeMillis()/1000).toString + "," + "a,a,a,a,a,a,a,a,a,a,a,a,a"))
  val auxStream: DStream[String] = ssc.queueStream[String](mutable.Queue(rdd1))
  auxStream.saveAsTextFiles("src/main/resources/data/pokemonAux/")

  def readFromSocketLast50() = {  // Conserva los n registros mas ecientes
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)
    val bufferStream: DStream[String] = ssc.textFileStream("src/main/resources/data/pokemonAux/")
    // transformation = lazy
    val unionStream: DStream[String] = socketStream
      .map(line => (System.currentTimeMillis()/1000).toString + "," + line)
      .union(bufferStream)
    val resultStream = unionStream
      .map(line => line.split(",").toSeq)
      .map(line => line(3))
      .filter(item => item.length > 2 && item !=  "Type 1")
      .countByValue() // aumenta un tipo debido a la lectura de los temporales

    unionStream
      .map(line => (line.split(","),line))
      .map(line => (line._1(0),line._2))
      .transform(rdd => {
        val list = rdd
          .map(item => item._1)
          .coalesce(1)
          .sortBy(item => item,false)
          .take(5)
          .toSeq
        list.foreach(println)
        rdd.filter {
          case(dateString: String, lineString: String) => list.contains(dateString)
        }
      })
      .map(line => line._2)
      .saveAsTextFiles("src/main/resources/data/pokemonAux/")

    // action
    resultStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocketLast50()
    // readFromSocket()
    // readFromFile()
  }
}
