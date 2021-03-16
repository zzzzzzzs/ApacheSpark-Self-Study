package spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operate_Action1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子 - reduce

        val list = List(4,6,5, 1,3,2)

        val rdd = sc.makeRDD(list)

//        val i: Int = rdd.reduce(_+_)
//        val d: Double = rdd.sum()
//        println("i = " + i)

        // TODO Spark - 行动算子 - collect
        // collect方法 会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        // 将Executor端的数据汇集到Driver端时，有可能内存不够用，会发生错误
        //rdd.collect()

        // TODO Spark - 行动算子 - count
        //val l: Long = rdd.count()
        //println(l)

        // TODO Spark - 行动算子 - first
        //val i: Int = rdd.first()
        //println(i)

        // TODO Spark - 行动算子 - take
        //val ints: Array[Int] = rdd.take(3)
        //println(ints.mkString(","))

        // TODO Spark - 行动算子 - takeOrdered
        // 先排序，再取前3个。
        val ints: Array[Int] = rdd.takeOrdered(3)
        println(ints.mkString(","))

        sc.stop()

    }
}
