import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    //创建spark streaming的StreamingContext,当然也可以通过sc来创建
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming")
    val ssc = new StreamingContext(conf,Seconds(10))
    /*
    接下来需要做这些事：
    1.Define the input sources by creating input DStreams.通过创建输入DStreams定义输入源
    2.Define the streaming computations by applying transformation and output operations to DStreams.通过对DStreams应用转换和输出操作来定义流计算。
    3.Start receiving data and processing it using streamingContext.start().开始接收数据并使用streamingContext.start()对其进行处理。
    4.Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().使用streamingContext.awaitTermination()等待进程停止(手动或由于任何错误)。
    5.The processing can be manually stopped using streamingContext.stop().可以使用streamingContext.stop()手动停止处理。
     */
    // 1.Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("master", 19999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate


  }
}
