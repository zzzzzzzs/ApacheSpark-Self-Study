package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark40_RDD_Operate_foldByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型 foldByKey
        //  当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
        // 取出每个分区内相同key的最大值然后分区间相加
        val list = List(
            ("a",1),("a",2),("c",3),
            ("b",4),("c",5),("c",6)
        )
        var rdd = sc.makeRDD(list,2)

        // TODO aggregateByKey : 根据Key聚合数据, 使用函数柯里化

        // wordcount
        // 如果分区内计算规则和分区间计算规则相同的场合，类似于reduceByKey
//        val newRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
//            (x, y) => x + y,
//            (x, y) => x + y
//        )

        // 如果分区内计算规则和分区间计算规则相同的场合，Spark提供了一个简洁的方法
        // foldByKey wordcount

        // 1. groupBy
        // 2. groupByKey
        // 3. reduceByKey
        // 4. aggregateByKey
        // 5. foldByKey
        val newRDD1: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
        newRDD1.collect.foreach(println)


        sc.stop()
    }
}
