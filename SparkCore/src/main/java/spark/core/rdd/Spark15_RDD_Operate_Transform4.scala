package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operate_Transform4 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List(1,2,3,4)
        // TODO 获取分区数据的同时，想要获取每个数据所在的分区编号
        // 1 => (0, 1)
        // 2 => (0, 2)
        // 3 => (1, 3)
        // 4 => (1, 4)
        var rdd = sc.makeRDD(list,2)
        // mapPartitionsWithIndex : (Int, iterator) => iterator
        var newRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(
                    num => (index, num)
                )
            }
        )
        newRDD.collect().foreach(println)

        sc.stop
    }
}
