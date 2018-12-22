/*
 * CopyRight benny
 * ProjectName: mybatis-generator-learn
 * Author: benny
 * Date: 18-12-22 下午11:15
 * LastModified: 18-12-22 下午11:15
 */

package cn.com.benny.learn.streaming


import cn.com.benny.learn.streaming.model.Order
import com.alibaba.fastjson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
  * <p>Description:<p>
  * QQ: 178542285
  *
  * @Author benny
  * @Date: 2018/12/22
  * @Time: 23:15
  */
object DealOrder {
    val brokers = "192.168.1.111:9092"
    val topics = "order"

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
//        val sc: SparkContext = new SparkContext(sparkConf)
//        sc.setLogLevel("INFO")
        val ssc = new JavaStreamingContext(sparkConf,Durations.seconds(1))
        var topicsSet = Set(topics);
        //kafka相关参数，必要！缺了会报错
        var kafkaParams:Map[String,String] = Map(
            "metadata.broker.list" -> brokers,
            "bootstrap.servers"-> brokers,
            "group.id"->"group1",
            "key.serializer"-> "org.apache.kafka.common.serialization.StringSerializer",
            "key.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer")
        //Topic分区  也可以通过配置项实现
        //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
        //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        //kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit",false);
        val partition: TopicPartition = new TopicPartition(topics,0)
        var offsets:mutable.HashMap[TopicPartition,Long] = mutable.HashMap(partition -> 2L)
        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        val lines: JavaInputDStream[ConsumerRecord[String, String]] = KafkaUtils
            .createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe(topicsSet,kafkaParams,offsets)
            )

//        lines.dstream.foreachRDD(par=>{
//            par.foreachPartition(println)
//        })

        ssc.checkpoint("G:\\Temp")
        val value: DStream[Long] = lines.dstream.map(a=>{
            a.value()

        }).
            countByWindow(Durations.seconds(3),Durations.seconds(1))

        value.foreachRDD(rdd=>{
            println(rdd.sum())
        })




        ssc.start()
        ssc.awaitTermination()
        ssc.close()


    }
}
