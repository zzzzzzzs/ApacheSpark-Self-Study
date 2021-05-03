package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operate_groupBy {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO group by :根据指定的规则对数据进行分组

        var list = List(1,2,3,4)
        var rdd = sc.makeRDD(list)

        // 0,1
        rdd.groupBy( num=>num%2 ).collect.foreach(println)


        sc.stop
    }
}
