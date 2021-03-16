package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operate_Action3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子 - aggregate
        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list,3)
        // aggregateByKey : 初始值用于分区内第一个key对应的value的计算
        // aggregate : 初始值不仅仅用于分区内计算，也用于分区间的计算
        val i: Int = rdd.aggregate(10)(_+_, _+_)
        println(i)

        sc.stop()

    }
}
