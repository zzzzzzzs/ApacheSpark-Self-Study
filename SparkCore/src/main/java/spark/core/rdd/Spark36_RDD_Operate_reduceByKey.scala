package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark36_RDD_Operate_reduceByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
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
