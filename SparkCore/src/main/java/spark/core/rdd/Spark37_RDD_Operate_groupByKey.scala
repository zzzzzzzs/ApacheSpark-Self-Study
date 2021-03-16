package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark37_RDD_Operate_groupByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(
            ("nba", "xxxxxx"),
            ("cba", "xxxxxx"),
            ("wnba", "xxxxxx"),
            ("nba", "yyyyyy")
        )

        val rdd = sc.makeRDD(list)

        // groupBy : 根据指定的规则进行分组，分组的key是通过计算获得
        //           将数据作为整体放置在一个组中
        //val groupByRDD: RDD[(String, Iterable[(String, String)])] = rdd.groupBy(_._1)
        //rdd.groupBy()
        // groupByKey : 根据数据的K值进行分组，Key相同，对应Value就放置在一个组中
        val newRDD: RDD[(String, Iterable[String])] = rdd.groupByKey()
        newRDD.collect().foreach(println)


        sc.stop()
    }
}
