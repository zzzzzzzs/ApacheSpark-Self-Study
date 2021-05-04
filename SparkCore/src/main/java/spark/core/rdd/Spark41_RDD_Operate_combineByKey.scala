package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark41_RDD_Operate_combineByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型 combineByKey
        // 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
        // 求每个key的平均值
        val list = List(
            ("a", 88), ("b", 95), ("a", 91),
            ("b", 93), ("a", 95), ("b", 98)
        )
        // ( "a", (88, 91) ) => ("a", (179,2)), ("b", (95,1) )
        // ( "b", (93,98))   => ("b", (191,2)), ("a",(95,1) )
        // =>
        // (a, (274, 3)), (b, (286, 3))
        // (a, 274/3), (b, 286/3)

        // 88 => (88,1) + 91 => (179,2)
        //                      (95,1)
        val rdd = sc.makeRDD(list,2)

        // TODO combineByKey可以实现数据类型不一致时的分区内计算和分区间计算
        //      combineByKey有一个参数列表，其中包含3个参数
        //      第一个参数表示：为了方便统计，将第一个key出现的值转换结构
        //      第二个参数表示：分区内的计算规则
        //      第三个参数表示：分区间的计算规则
        val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            num => (num, 1),
            (x: (Int, Int), y) => {
                (x._1 + y, x._2 + 1)
            },
            (x: (Int, Int), y: (Int, Int)) => {
                (x._1 + y._1, x._2 + y._2)
            }
        )
        newRDD.mapValues(t=>{t._1/t._2}).collect.foreach(println)

        // TODO aggregateByKey可以实现，但是稍微有一点麻烦。
        //      foldByKey
        //rdd.aggregateByKey()

        // TODO reduceByKey可以实现两两聚合，但是要求处理的数据类型和value数据类型保持一致
        // rdd.reduceByKey(_+_)

        // TODO groupByKey可以实现功能，但是性能太低，所以不推荐使用
//        val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
//        newRDD.mapValues(
//            iter => {
//                val list = iter.toList
//                list.sum / list.size
//            }
//        ).collect().foreach(println)


        sc.stop()
    }
}
