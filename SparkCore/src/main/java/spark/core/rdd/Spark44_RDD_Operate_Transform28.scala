package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark44_RDD_Operate_Transform28 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(
            ("a", 1), ("c",2), ("b", 3)
        )
        val list1 = List(
            ("a",5), ("b", 6),("a", 4)
        )
        // join : 两个RDD，相同的key会将value连接在一起
        val rdd1 = sc.makeRDD(list)
        val rdd2 = sc.makeRDD(list1)

        // join 存在笛卡尔乘积，所以中间处理的数据量会非常的大，影响效率
        // 内连接
        //rdd1.join(rdd2).collect().foreach(println)

        // 左外连接
        //rdd1.leftOuterJoin(rdd2).collect().foreach(println)
        rdd1.rightOuterJoin(rdd2).collect().foreach(println)

        sc.stop()
    }

}
