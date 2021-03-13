package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {

    def main(args: Array[String]): Unit = {

        // TODO Spark -  RDD的创建
        // 1. 创建方式：
        //    1.1 内存中创建 : List(1,2,3,4)
        //    1.2 存储中创建 : File
        //    1.3 从RDD创建
        //    1.4 直接new

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark环境对象从集合中创建RDD
        val list = List(1,2,3,4)

        // 使用parallelize方法将集合转换为RDD，进行操作
        // parallelize : 并行处理集合中的数据
        // Scala集合：List.par
        //val numRDD: RDD[Int] = sc.parallelize(list)
        //numRDD.collect().foreach(println)

        // makeRDD : 生成RDD对象
        // makeRDD的底层代码就是调用parallelize方法，所以逻辑上没有区别
        val numRDD: RDD[Int] = sc.makeRDD(list)
        numRDD.collect().foreach(println)


        sc.stop
    }
}
