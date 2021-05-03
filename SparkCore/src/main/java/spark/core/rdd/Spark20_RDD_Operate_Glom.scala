package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operate_Glom {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Glom:将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
        //      小练习：取每个分区的最大值后求和
        // (1,2),(3,4),(5,6,7)
        // 2,4,7
        // 13
        val list = List(1,2,3,4,5,6,7)
        var rdd : RDD[Int] = sc.makeRDD(list,3)

        // (1,2) => 2
        // (3,4) => 4
        // (5,6,7) => 7
        val rdd1: RDD[Array[Int]] = rdd.glom()

        val rdd2 : RDD[Int] = rdd1.map(
            array => {
                array.max
            }
        )
        val ints: Array[Int] = rdd2.collect()
        println(ints.sum)

//        rdd1.collect().foreach(
//            array => println(array.mkString(", "))
//        )


        sc.stop
    }
}
