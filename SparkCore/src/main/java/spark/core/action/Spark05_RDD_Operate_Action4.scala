package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operate_Action4 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子 - fold
        val list = List(1,2,3,4)
        val list1 = List(
            "hello", "Spark", "hello", "Spark"
        )
        val rdd = sc.makeRDD(list1,2)

//        val sum = rdd.fold( 10 ) (_+_)
//        println(sum)

        // TODO Spark - 行动算子 - countByKey
        // wordcount
//        val intToLong: collection.Map[String, Long] = rdd.map( (_,1) ).countByKey()
//        println(intToLong)

        // TODO Spark - 行动算子 - countByValue
        // wordcount
        // Value类型RDD，双Value类型RDD, KV类型RDD
        val stringToLong: collection.Map[String, Long] = rdd.countByValue()
        println(stringToLong)

        sc.stop()

    }
}
