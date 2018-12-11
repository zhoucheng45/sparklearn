package cn.com.benny.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>Description:<p>
  * QQ: 178542285
  *
  * @Author benny
  * @Date: 2018/12/11
  * @Time: 0:52
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        print("efsd")
        val spark: SparkSession = SparkSession.builder.master("local")
            .appName("hello-wrold").config("spark.some.config.option", "some-value")
            .getOrCreate

        val value: RDD[String] = spark.sparkContext.textFile("data/word.txt")
        value.flatMap(line => line.split(" "))
            .map(word => (word,1))
            .reduceByKey(_ + _).collect().sor
            .sortWith((a,b)=>{
            a._2>b._2
        }).foreach(println)



    }
}
case class Person(name:String, age:Int, gender:String);
