package cn.com.benny.learn

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
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
        val spark: SparkSession = SparkSession.builder.master("local")
            .appName("hello-wrold").config("spark.some.config.option", "some-value")
            .getOrCreate

        //        readLocalTextFile

//        val seq = List(1, 2, 3, 4, 5, 6)
//        val unit: RDD[Int] = spark.sparkContext.parallelize(seq)
//        unit.foreach(println)

        readMysql(spark)
    }

    def readMysql(sparkSession: SparkSession)={

        val relation: DataFrame = sparkSession.read.jdbc(MysqlCfg.connUrl,"help_relation",MysqlCfg.properties)
        val topic: DataFrame = sparkSession.read.jdbc(MysqlCfg.connUrl,"help_topic",MysqlCfg.properties)
        val keyword: DataFrame = sparkSession.read.jdbc(MysqlCfg.connUrl,"help_keyword",MysqlCfg.properties)
        val value: DataFrame = topic.join(relation,topic("help_topic_id1")===relation("help_topic_id"))
        val frame: DataFrame = value.join(keyword, value("help_keyword_id") === keyword("help_keyword_id"))
        val a: RDD[(String, Iterable[Long])] = frame.rdd.map(row =>{
            (row.getAs[String]("name"),row.getAs[Long]("help_topic_id1"))
        }).groupByKey()

        val result: RDD[(String, Long)] = a.flatMap{
            case (item,iter)=>{
                iter.toList.sortWith((a,b)=>{
                    a > b
                }).take(3).map(item2=>(item,item2))

            }
        }
        result.collect().foreach(e =>{
            println(s"${e._1} : ${e._2}")
        })

    }

    def readLocalTextFile(spark: SparkSession) = {
        val value: RDD[String] = spark.sparkContext.textFile("data/word.txt")
        value.flatMap(line => line.split(" ")).filter(word => word.length > 0)
            .map(word => (word, 1))
            .reduceByKey(_ + _).collect()
            .sortWith((a, b) => {
                a._2 > b._2
            }).foreach(println)
    }
}

case class Person(name: String, age: Int, gender: String);
