package spark.core.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Dep {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // RDD 的toDebugString方法可以查看当前RDD的血缘关系。不是依赖关系

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list)
        //println(rdd.toDebugString)
        println(rdd.dependencies)
        println("******************")

        val mapRDD = rdd.map((_,1))
        //println(mapRDD.toDebugString)
        println(mapRDD.dependencies)
        println("******************")

        val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)
        //println(reduceRDD.toDebugString)
        println(reduceRDD.dependencies)
        println("******************")

        reduceRDD.collect.foreach(println)

        sc.stop()

    }
}
