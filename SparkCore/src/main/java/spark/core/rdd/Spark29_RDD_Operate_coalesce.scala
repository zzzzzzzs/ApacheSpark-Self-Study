package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark29_RDD_Operate_coalesce {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - coalesce (缩减,合并分区)
        // 将多个分区减少分区，提高效率
        val list = List(1,2,3,4,5,6,7,8,9,10)

        // 8 / 4 = 2
        val rdd = sc.makeRDD(list,10)

        // 缩减分区
        // 1. 合并分区时，会不会将数据打乱重新组合，是否存在shuffle操作？
        //    不会进行数据shuffle操作，就是简单的分区合并
        //    如果合并后，可能会产生数据倾斜的问题，为了避免这个问题，可以采用shuffle
        //    使用shuffle后，会将各个分区的数据变得更加均衡（不是均匀）
        // 2. 如果缩减分区的数值大于原始分区数量，会出现什么情况？

        // coalesce算子的第二个参数表示是否采用shuffle操作，默认为false
        val newRDD1: RDD[Int] = rdd.coalesce(2)
        val newRDD2: RDD[Int] = rdd.coalesce(2, true)

        newRDD1.saveAsTextFile("output")
        newRDD2.saveAsTextFile("output1")





        sc.stop
    }
}
