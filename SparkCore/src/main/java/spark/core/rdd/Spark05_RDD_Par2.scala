package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Par2 {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 文件数据的分区

        // 读取文件时默认分区数量为 ： math.min(defaultParallelism, 2) => 2
        // textFile的第二个参数表示最小分区的数量(不见得相等)
        // defaultParallelism = scheduler.conf.getInt("spark.default.parallelism", totalCores)
        val file: RDD[String] = sc.textFile("input/aaa.txt")

        file.saveAsTextFile("output")

        sc.stop
    }
}
