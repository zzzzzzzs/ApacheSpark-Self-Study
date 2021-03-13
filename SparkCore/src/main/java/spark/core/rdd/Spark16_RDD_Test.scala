package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 小练习：获取第二个数据分区的数据
        val list = List(1,2,3,4,5,6)
        // 分区索引编号从0开始
        // 获取的数据应该编号为1
        val rdd = sc.makeRDD(list,3)
        // mapPartitionsWithIndex : (index, iterator) => iterator
        val newRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                   // null
                    Nil.iterator
                    //iter.filter(x=>false)
                }
            }
        )
        newRDD.collect().foreach(println)

        //val ints: List[Int] = List(1,2,3,4).filter(x=>false)

        sc.stop
    }
}
