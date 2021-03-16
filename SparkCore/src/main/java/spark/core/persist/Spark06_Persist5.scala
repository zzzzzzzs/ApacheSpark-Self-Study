package spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Persist5 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("Hello", "Scala", "Hello"))

        val mapRDD = rdd.map(word=>{
            println("map....")
            (word,1)
        })

        // cache方法返回的就是当前的RDD对象，所以在使用时，可以使用返回结果，也可以不使用
        mapRDD.cache()
        val cacheRDD: RDD[(String, Int)] = mapRDD.cache()

        println(cacheRDD.collect.mkString(","))
        println("*************************")
        println(cacheRDD.collect.mkString(","))


        sc.stop()

    }
}
