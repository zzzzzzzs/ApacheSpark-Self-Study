package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operate_Transform3 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List(1,4,3,2)
        // (1,4),(3,2)
        // =>
        // 4, 3
        val rdd: RDD[Int] = sc.makeRDD(list,2)

//        val newRDD = rdd.mapPartitions(
//            iter => {
//                iter.filter(_%2 == 0)
//            }
//        )

        // TODO 小练习：获取每个数据分区的最大值
        // mapPartitions : iterator => iterator
        val newRDD = rdd.mapPartitions(
            iter => {  // scala  iterator max min sum
                List(iter.max).iterator
            }
        )

        newRDD.collect().foreach(println)

        sc.stop
    }
}
