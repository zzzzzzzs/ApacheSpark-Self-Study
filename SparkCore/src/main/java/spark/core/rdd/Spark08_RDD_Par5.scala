package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Par5 {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 在加载数据时，可以设定并行度来设置分区数量
        // Spark中读取文件数据，其实采用的是Hadoop文件读取方式。
        // TODO 1. 分区数量到底是多少？

        //    Spark是不能决定具体的分区数量，只能提供最小分区数量
        //    具体的分区数量是由hadoop在读取文件时自动判断的。 public InputSplit[] getSplits(JobConf job, int numSplits)
        //        文件总字节大小： totalsize = 7byte (有回车换行)
        //        每个分区应该读取的字节大小：goalSize = totalsize / num = 1
        //                                           7 / 3 = 2....1
        //        使用总的字节数除以分区数量，看余数字节数和每个分区字节数的比率是否超过10%（1.1倍），如果超过，需要一个新的分区
        //     TODO 最终分区数量 = 最小分区数（3） + 可能的分区数量（0，1）

        // TODO 2. 每个分区存储什么数据？

        //    Spark不决定数据如何存储，依然是由Hadoop来决定
        //    2.1 Hadoop读取文件是一行一行读取的，不是按照字节的方式.
        //    2.2 Hadoop读取数据是按照数据的偏移量（字节数）的读取的，偏移量从0开始的，偏移量不会被重复读取

        /*
          数据读取时以偏移量（字节数）为单位
          @@表示回车换行（占用2个字节）
          数据：   偏移量
          1@@  => 0,1,2
          2@@  => 3,4,5
          3    => 6

          如果minPartitions不设置则就是2，那么此时分区数就是2，但由于是文件会按照hadoopFile里面做分区，使用了Hadoop的分区规则
          7 / 2 = 3...1 => 2 + 1 => 3
          数据分区的偏移量范围的计算：
          0 => 0 + 3 => 0 - 3 => 【1,2】
          1 => 3 + 3 => 3 - 6 => 【3】
          2 => 6 + 1 => 6 - 7 => 【】



          7 / 3 = 2...1 => 3 + 1 = 4

          // 0 + 2 => 0 - 2 => 【1】
          // 2 + 2 => 2 - 4 => 【2】
          // 4 + 2 => 4 - 6 => 【3】
          // 6 + 1 => 6 - 7 => 【】



         */

        //val rdd1: RDD[String] = sc.textFile("input/aaa.txt", 2)
        val rdd2: RDD[String] = sc.textFile("SparkCore/datas/aaa.txt", 2)
        //rdd1.saveAsTextFile("output1")
        rdd2.saveAsTextFile("./SparkCore/output")

        sc.stop
    }
}
