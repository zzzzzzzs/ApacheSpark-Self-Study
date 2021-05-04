package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_Operate_sortBy {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - sortBy
        val list = List(1,3,6,5,4,2)
        //list.sortBy(num=>num)(Ordering.Int.reverse)

        // 8 / 4 = 2
        val rdd = sc.makeRDD(list,2)

        // 根据指定的规则对处理的数据进行排序
        // sortBy的第二个参数表示排序的方式
        //    默认的排序规则为升序，取值为true, 如果设置为false，那么降序
//        rdd.sortBy(num=>num, false).collect().foreach(println)

        // 执行结果，全局排序。数据在排序的过程中会将数据打乱重新组合，分区数量还是原来的，所以sortBy也包含shuffle操作。

        // 如果分区内的数据是否会排序
        // （1，3，6），（5，4，2）
        // 1,3,6, 2,4,5
        // 需要用别的算子组合完成分区内排序
        rdd.sortBy(num=>num).saveAsTextFile("./SparkCore/output")

        sc.stop
    }
}
