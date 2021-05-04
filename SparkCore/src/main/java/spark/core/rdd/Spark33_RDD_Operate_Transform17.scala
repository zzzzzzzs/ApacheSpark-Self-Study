package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark33_RDD_Operate_Transform17 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - 双Value union intersection subtract zip
        // 所谓的双value其实就是两个RDD的操作

        // 交集，并集，差集
        val list1 = List(1,2,3,4)
        val list2 = List(3,4,5,6)
        val list3 = List("3","4","5","6")

        val rdd1 = sc.makeRDD(list1,2)
        val rdd2 = sc.makeRDD(list2,2)
        val rdd3 = sc.makeRDD(list3,2)

        // 并集
        // 如果类型不匹配，两个RDD无法形成并集
        //println(rdd1.union(rdd2).collect().mkString(","))
        //println(rdd1.union(rdd3).collect().mkString(",")) // (X)
        // 交集
        // 如果类型不匹配，两个RDD无法形成交集
        //println(rdd1.intersection(rdd2).collect().mkString(","))
        //println(rdd1.intersection(rdd3).collect().mkString(",")) // (X)
        // 差集
        // 如果类型不匹配，两个RDD无法形成差集
        //println(rdd1.subtract(rdd2).collect().mkString(","))
        //println(rdd1.subtract(rdd3).collect().mkString(",")) // (X)

        // 拉链（zip）
        // 1. 如果其中一个数据量多？
        //   Can only zip RDDs with same number of elements in each partition
        //   Can't zip RDDs with unequal numbers of partitions: List(2, 3)
        //   只有相同分区数量，每个分区数量相同的两个RDD才能进行拉链操作
        // 2. 如果类型不匹配，是否可以拉链
        //    可以进行拉链操作，返回的结果类型，K为第一个RDD的数据类型，V为第二个RDD的数据类型
        //println(rdd1.zip(rdd2).collect().mkString(","))
        println(rdd1.zip(rdd3).collect().mkString(","))

        sc.stop
    }
}
