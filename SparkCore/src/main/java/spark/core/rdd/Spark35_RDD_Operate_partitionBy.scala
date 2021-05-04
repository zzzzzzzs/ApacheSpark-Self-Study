package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark35_RDD_Operate_partitionBy {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(
            ("nba", "xxxxxx"),
            ("cba", "xxxxxx"),
            ("wnba", "xxxxxx"),
            ("nba", "yyyyyy")
        )

        val rdd = sc.makeRDD(list,2)
        // TODO 改变数据的分区
        // partitionBy如果在分区器前后一致的情况下，不会进行分区操作，也就不会shuffle。所以一般都会有shuffle。
        val newRDD: RDD[(String, String)] = rdd.partitionBy( new MyPartitioner() )

        newRDD.saveAsTextFile("./SparkCore/output")

        sc.stop
    }
    // 自定义分区器
    //  1. 继承Partitioner
    //  2. 重写方法
    class MyPartitioner extends Partitioner{
        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
            // 根据数据的Key决定分区号
            key match {
                case "nba" => 0
                case "cba" => 1
                case "wnba" => 2
            }
        }
    }
}
