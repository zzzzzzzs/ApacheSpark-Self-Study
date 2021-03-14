package spark.core.rdd

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Operate_filter {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Filter
        // 根据指定的规则对数据进行筛选过滤，如果条件满足，数据保留，如果条件不满足，数据丢弃
//        val list = List(1,2,3,4)
//        val rdd = sc.makeRDD(list)
//
//        rdd.filter(num=>num%2==0).collect().foreach(println)

        // TODO 小练习：从服务器日志数据apache.log中获取2015年5月17日的请求路径
        val rdd = sc.textFile("input/apache.log")

        rdd.filter(
            line => {
                val datas = line.split(" ")
                val dateString = datas(3)
                // 17/05/2015:10:05:03
                dateString.startsWith("17/05/2015")

            }
        ).collect.foreach(println)

        sc.stop
    }
}
