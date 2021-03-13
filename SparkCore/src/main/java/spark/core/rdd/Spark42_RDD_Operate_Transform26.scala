package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark42_RDD_Operate_Transform26 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        // 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
        // 求每个key的平均值
        val list = List(
            ("a", 88), ("b", 95), ("a", 91),
            ("b", 93), ("a", 95), ("b", 98)
        )

        val rdd = sc.makeRDD(list)

        /*

combineByKeyWithClassTag

reduceByKey =>
    combineByKeyWithClassTag
          // 分区内第一个key如何处理
          createCombiner: V => C,     ==> (v: V) => v
          // 分区内计算
          mergeValue: (C, V) => C,    ==> (V, V) => V
          // 分区间计算
          mergeCombiners: (C, C) => C ==> (V, V) => V

aggregateByKey =>
    combineByKeyWithClassTag
          // 将zeroValue和处理数据的第一个值进行分区内计算
          createCombiner: V => C,     ==> (v: V) => cleanedSeqOp(createZero(), v)
          // 分区内计算
          mergeValue: (C, V) => C,    ==> cleanedSeqOp
          // 分区间计算
          mergeCombiners: (C, C) => C ==> combOp

foldByKey =>
    combineByKeyWithClassTag
          // 将zeroValue和处理数据的第一个值进行分区内计算
          createCombiner: V => C,     ==> (v: V) => cleanedFunc(createZero(), v)
          // 分区内计算
          mergeValue: (C, V) => C,    ==> cleanedFunc
          // 分区间计算
          mergeCombiners: (C, C) => C ==> cleanedFunc

combineByKey =>
    combineByKeyWithClassTag
          // 将zeroValue和处理数据的第一个值进行分区内计算
          createCombiner: V => C,
          // 分区内计算
          mergeValue: (C, V) => C,
          // 分区间计算
          mergeCombiners: (C, C) => C


         */
        //rdd.reduceByKey()
        //rdd.aggregateByKey()
        //rdd.foldByKey()
        //rdd.combineByKey()


        sc.stop()
    }
}
