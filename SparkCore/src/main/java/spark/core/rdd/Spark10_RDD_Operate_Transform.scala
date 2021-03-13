package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operate_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - map
        // 类似于Scala集合中的map方法
        // 将集合中的每一个元素转换成一个元素放置在新的集合中
        //val list = List(1,2,3,4)
        //val newList: List[Int] = list.map(_*2)

        // RDD 中map算子可以将旧的RDD转换为新的RDD
        //  可以将旧的数据的处理按照新的处理方式进行转换
//        val list = List(1,2,3,4)
//        val rdd: RDD[Int] = sc.makeRDD(list)
//        val newRDD: RDD[Int] = rdd.map(_*2)
//        newRDD.collect().foreach(println)

        // TODO RDD通过转换算子操作后，会产生新的RDD
        // 新的RDD的分区数量默认和旧的RDD的分区数量保持一致
        // 数据处理完成后所在新的RDD分区和旧的RDD分区保持一致
        val list = List(1,2,3,4)
        val rdd: RDD[Int] = sc.makeRDD(list,3)
        val newRDD: RDD[Int] = rdd.map(_*2)

        rdd.saveAsTextFile("output1")
        newRDD.saveAsTextFile("output2")

        sc.stop
    }
}
