package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operate_map {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)


        // 运行的结果是并行的关系
        val list = List(1,2,3,4)
        val rdd: RDD[Int] = sc.makeRDD(list,2)
        val newRDD: RDD[Int] = rdd.map(
            num => {
                println("现在执行的是第一个map :" + num)
                num
            }
        )
        val newRDD1: RDD[Int] = newRDD.map(
            num => {
                println("现在执行的是第二个map :" + num)
                num
            }
        )
        newRDD1.collect()

        sc.stop
    }
}
