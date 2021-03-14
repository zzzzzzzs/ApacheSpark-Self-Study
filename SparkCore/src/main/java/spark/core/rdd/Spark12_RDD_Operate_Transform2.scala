package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operate_Transform2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List(1,2,3,4)
        val rdd: RDD[Int] = sc.makeRDD(list,2)

        // map算子分区内的每一条数据逻辑全部处理完成才会执行下一条,效率比较低
        // 为了提供数据处理的效率，可以将数据一次性地发送到Executor来执行

        // mapPartitions算子可以将一个分区的数据当成一个整体发送到executor执行

        // TODO mapPartitions数据处理性能要比map高
        //      mapPartitions数据处理的规则是：必须保证这个分区的所有数据处理完成
        //      数据才会释放，如果没有全部执行完，那么不会释放数据，可能会出现内存溢出的问题。
        //  map 适合用在内存小的情况下
        val newRDD1 = rdd.map(
            num => {
                println("************************")
                num * 2
            }
        )

        val newRDD = rdd.mapPartitions(
            iter => {
                println("######################")
                iter.map(_*2)
            }
        )

        newRDD.collect()//.foreach(println)
        newRDD1.collect()//.foreach(println)

        sc.stop
    }
}
