package cn.com.benny.learn.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * <p>Description:<p>
  * QQ: 178542285
  *
  * @Author benny
  * @Date: 2018/12/17
  * @Time: 0:43
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        val sparkCfg: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)

        val streamingContext: StreamingContext = new StreamingContext(sparkCfg, Seconds(5))
        val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.1.111",9999)
        val words: DStream[String] = lines.flatMap(_.split(" "))
        words.map(word =>{(word,1)}).reduceByKey(_ + _)
            .print()


        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
