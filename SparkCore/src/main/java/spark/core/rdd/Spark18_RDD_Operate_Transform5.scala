package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operate_Transform5 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List(
            List(1,2),List(3,4)
        )

        val list1 = List(1,2,3,4)

        val list2 = List("hello world")

        val rdd = sc.makeRDD(list2)
        //rdd.flatMap(list=>list).collect().foreach(println)
        //rdd.flatMap(_).collect().foreach(println) (X)
//        rdd.flatMap(
//            num => List(num)
//        ).collect().foreach(println)

        rdd.flatMap(s=>s).collect.foreach(println)


        sc.stop
    }
}
