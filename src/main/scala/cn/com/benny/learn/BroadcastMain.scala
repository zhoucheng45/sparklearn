package cn.com.benny.learn

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * <p>Description:<p>
  * QQ: 178542285
  *
  * @Author benny
  * @Date: 2018/12/16
  * @Time: 20:46
  */
object BroadcastMain {

    def main(args: Array[String]): Unit = {
        var sparkSession: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
            .master("local[*]").getOrCreate()

        val broadcast: Broadcast[ArrayBuffer[String]] = sparkSession.sparkContext.broadcast(ArrayBuffer("String", "Int"))


    }
}
