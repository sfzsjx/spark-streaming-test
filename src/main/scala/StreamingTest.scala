

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object StreamingTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingTest").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val lines = ssc.socketTextStream("master", 19999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
} 