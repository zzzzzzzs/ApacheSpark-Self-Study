package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark36_RDD_Operate_reduceByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        /*TODO Spark - 转换算子 - (KV)类型 reduceByKey
            存在shuffle（一定落盘），reduceByKey在shuffle之间进行预聚合（combiner  称之为分区内聚合，也就是预聚合mapSideCombine: Boolean = true）
            ，这样落盘的数量就少了，IO的次数也就变少了，性能提高了
         */
        val list = List(
            ("nba", "xxxxxx"),
            ("cba", "xxxxxx"),
            ("wnba", "xxxxxx"),
            ("nba", "yyyyyy")
        )

        val rdd = sc.makeRDD(list)
        // 针对相同的key进行value的聚合
        // 分组 + 聚合
        rdd.reduceByKey(_ + _ ).collect().foreach(println)

    }
}
