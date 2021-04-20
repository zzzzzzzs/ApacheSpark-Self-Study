package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Par3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 在加载数据时，可以设定并行度来设置分区数量
       // val list = List(1,2,3,4)
        // 使用makeRDD时，不传递第二个参数，那么会默认配置并行度
        // defaultParallelism ： totalCores : 8
        //var rdd1 = sc.makeRDD(list)

        // TODO 读取文件时，并行度设置（分区设置）
        // 如果textFile不传递第二个参数，那么默认使用：minPartitions: Int = defaultMinPartitions
        // math.min(defaultParallelism, 2)
        // math.min(8, 2) => 2
        // 最终的结果3个分区
        val rdd2: RDD[String] = sc.textFile("input/aaa.txt")
        // 保存成分区文件
        rdd2.saveAsTextFile("output")

        sc.stop
    }
}
