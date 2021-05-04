package spark.core.accAndbc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 累加器
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(1,2,3,4),2
        )

        // RDD执行的效率和shuffle有很大的关系。当数据量很大，shuffle就会变慢，性能就会下降
        // 一般提升Spark的性能就是提升shuffle的性能：
        // 1. 数据量减少 （filter）
        // 2. 预聚合
        // 3. shuffle本身的IO

        // word-count(没有shuffle)
        // 算子：所谓的算子，其实就是RDD的方法
        //   map : RDD方法外和方法内部的代码的执行位置不一样。
        // Scala集合的方法不称之为叫算子
        //   map ：内存中的数据处理，不涉及到分布式计算的操作

        // sum变量由于闭包的原因，会随着数据和任务发给Executor进行计算
        // 但是计算完毕后。sum变量并不会返回到Driver端。所以Driver端的sum变量不会被更新
        // 所以取值一直为0，没有变化
        var sum = 0
        rdd.foreach(num=>{
            sum = sum + num
            //println("sum = " + sum)
        })

        // Driver
        println("sum = " + sum)

        sc.stop()
    }
}
