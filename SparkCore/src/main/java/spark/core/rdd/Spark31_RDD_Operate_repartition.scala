package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Operate_repartition {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - repartition (缩减,合并分区)
        // 将多个分区减少分区，提高效率
        val list = List(1,2,3,4,5,6,7,8,9,10)

        // 8 / 4 = 2
        val rdd = sc.makeRDD(list,2)

        // coalesce算子一般就用于缩减分区，而不是扩大分区。
        // Spark提供另外一个算子用于扩大分区 : repartition

        // repartition 其实就是使用shuffle的coalesce算子
        // 因为扩大分区一定要将数据打乱重新组合。

        // coalesce缩减分区时，默认不使用shuffle，但是想要数据均衡一些，可以使用shuffle

        //val newRDD1: RDD[Int] = rdd.coalesce(4)
        //val newRDD2: RDD[Int] = rdd.coalesce(4, true)
        rdd.repartition(5)

        //newRDD1.saveAsTextFile("output")
        //newRDD2.saveAsTextFile("output1")





        sc.stop
    }
}
