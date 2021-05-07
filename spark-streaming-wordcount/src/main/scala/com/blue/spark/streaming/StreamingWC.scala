package com.blue.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWC {

  def updateFunc(new_values:Seq[Int], last_state:Option[Int]): Option[Int] = {
    Some(new_values.sum + last_state.getOrElse(0))
}

  def createContext(ip: String, port: Int, checkpointDirectory: String) : StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName(StreamingWC.getClass.getName)
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(checkpointDirectory)

    val ds = ssc.socketTextStream(ip,port)
    val result = ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc=updateFunc)
    result.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val checkpointDirectory = "/tmp/checkpoint/spark-streaming"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createContext("127.0.0.1", 9999, checkpointDirectory))

    ssc.start()
    ssc.awaitTermination()
  }
}
