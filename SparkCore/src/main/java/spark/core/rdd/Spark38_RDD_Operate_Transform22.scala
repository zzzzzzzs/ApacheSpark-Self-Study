package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark38_RDD_Operate_Transform22 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(
            ("a", 1),
            ("b", 1),
            ("a", 1),
            ("b", 1)
        )

        val rdd = sc.makeRDD(list)

        // reduceByKey
        val wordToCount: RDD[(String, Int)] = rdd.reduceByKey(_+_)
        wordToCount.collect().foreach(println)

        println("*********************")
        // groupByKey
        val wordToCount1: RDD[(String, Int)] = rdd.groupByKey().mapValues(_.size)
        wordToCount1.collect().foreach(println)

        sc.stop()
    }
}
