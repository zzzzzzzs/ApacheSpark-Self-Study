package spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark00_Persist1 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        // 我们发现在分组操作的时候有的步骤是一样的，现在我们把一样的步骤注释掉，把对象重用起来
        // 这里是wordcount
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(
            w=>{
                println("@@@@@@@@@@@@@@@@@@@@@@@@@@")
                (w, 1)
            }
        )

        val reduceRDD = mapRDD.reduceByKey(_ + _)

        reduceRDD.collect().foreach(println)

        println("*********************************************")

        // 这是wordcount的分组操作
//        val list1 = List("Hello Scala", "Hello Spark")
//
//        val rdd1 = sc.makeRDD(list1)
//
//        val flatRDD1 = rdd1.flatMap(_.split(" "))
//
//        val mapRDD1 = flatRDD1.map((_, 1))
//
        val groupRDD = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        sc.stop()

    }
}
