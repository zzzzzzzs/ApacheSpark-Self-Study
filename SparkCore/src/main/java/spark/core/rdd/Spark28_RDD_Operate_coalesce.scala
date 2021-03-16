package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_RDD_Operate_coalesce {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - coalesce (缩减,合并分区)
        // 将多个分区减少分区，提高效率
        val list = List(1,2,3,4,5,6,7,8,9,10)

        // 8 / 4 = 2
        val rdd = sc.makeRDD(list,4)

        // 缩减分区
        val newRDD: RDD[Int] = rdd.coalesce(2)
        val newRDD1: RDD[Int] = rdd.coalesce(3)

        //rdd.saveAsTextFile("output1")
       // newRDD.saveAsTextFile("output2")
        newRDD1.saveAsTextFile("output3")

        // 1. 合并分区时，会不会将数据打乱重新组合，是否存在shuffle操作？
        //    不会进行数据shuffle操作，就是简单的分区合并
        //    如果合并后，可能会产生数据倾斜的问题，为了避免这个问题，可以采用shuffle
        // 2. 如果缩减分区的数值大于原始分区数量，会出现什么情况？



        sc.stop
    }
}
