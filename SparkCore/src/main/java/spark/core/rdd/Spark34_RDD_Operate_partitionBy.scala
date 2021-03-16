package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark34_RDD_Operate_partitionBy {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(1,2,3,4)
        val rdd  : RDD[Int] = sc.makeRDD(list,2)
        val rdd1 : RDD[(Int, Int)] = rdd.map( (_,1) )

        // RDD中默认没有partitionBy方法，所以无法调用
        //rdd.partitionBy
        // Scala编译器可以在编译出现错误的场合下，尝试进行规则查找，找到可以让程序编译通过的规则（隐式转换规则）
        // Spark提供了隐式转换能力：RDD => PairRDDFunctions
        // PairRDDFunctions类中提供了很多面向KV类型数据的处理方法.

        // partitionBy根据指定的规则对数据进行重分区
        // repartition表示分区的变化 ：3 => 2 => 6
        //rdd1.repartition()
        // partitionBy更强调数据重新分区
        // partitionBy 传递的参数是分区器: Partitioner
        //     Spark默认提供了三个分区器，主要是2个：Hash, Range
        //     Spark默认采用Hash分区器
        // partitionBy存在shuffle操作

        // Range分区器一般在排序时使用。平时使用多个不多。
        val rdd2 = rdd1.partitionBy( new HashPartitioner(2) )


        rdd1.saveAsTextFile("output1")
        rdd2.saveAsTextFile("output2")




        sc.stop
    }
}
