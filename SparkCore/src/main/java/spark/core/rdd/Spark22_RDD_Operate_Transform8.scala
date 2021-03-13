package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operate_Transform8 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO group by
        // groupBy会根据分组规则将数据放置在不同的组中
        // 那么这样就会导致数据在执行过程被打乱重新组合
        // 这个操作我们称之为shuffle
        // 即使打乱重新组合了，那么数据依然可能全部都放置在一个分区中.不一定是平均分。
        var list = List(1,2,3,4,5,6)
        var rdd = sc.makeRDD(list,2)

        //rdd.groupBy( num=>num%2 ).saveAsTextFile("output")

        // 默认情况下，RDD的转换操作，分区数量应该是相同的，但是如果要改变数量，是可以。
        // 改变分区时，groupBy匿名函数的类型不能省略
        rdd.groupBy(
            (x:Int) => x % 3,
            2
        ).saveAsTextFile("output")

        sc.stop
    }
}
