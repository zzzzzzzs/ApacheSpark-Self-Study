package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD1 {

    def main(args: Array[String]): Unit = {

        // TODO Spark -  RDD的创建
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark环境对象从存储系统中创建RDD
        // 所谓的存储系统，基本上就是文件系统，数据库，Hbase
        // path 表示文件的相对路径
        // Spark环境通过textFile来读取文件，读取的方式一行一行来读取的。
        // 路径可以使用星号进行通配操作 : input/word*.txt
        // path路径可以是具体的文件，也可以是目录
        val file: RDD[String] = sc.textFile("input")

        file.collect().foreach(println)


        sc.stop
    }
}
