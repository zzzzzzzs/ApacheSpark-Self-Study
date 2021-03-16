package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operate_Action6 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子 - foreach
        val list = List(3,4,1,2)

        val rdd = sc.makeRDD(list,2)

        // 这里是在 Driver 端打印的
        val ints: Array[Int] = rdd.collect()
        ints.foreach(println)
        println("*************************")


        // 所谓的算子其实就是RDD的方法，但是由于存在分布式的计算能力，所以为了
        // 和Scala集合的方法区分开，才称之为算子。
        // Driver(调度) <=> Executor(计算)
        // 算子外部的代码的执行位置是Driver端
        // 算子内部的代码的执行位置是Executor端

        // 分布式遍历
        // Coding
        // 执行计算时，会将匿名函数作为任务的一部分传递给Executor执行。
//        val i = 10
//        rdd.foreach(println(i))

        sc.stop()

    }
}
