package spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc1 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 累加器
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(1,2,3,4),2
        )

        // 使用累加器进行数据的操作

        // 所谓的累加器，其实就是告诉Spark计算的时候，需要变量从Driver端传递到Executor端进行计算
        // 计算完毕后，Spark会将每一个Executor的计算结果返回到Driver端进行聚合

        // TODO 累加器：分布式共享只写变量

        // 这里的只写的概念是多个Executor节点无法互相方法累加器的值，只能更新
        // 当计算完毕时，Driver可以访问每一个节点的累加器的结果

        // 创建累加器
        val sum = sc.longAccumulator("sum")
        //sc.doubleAccumulator
        //sc.collectionAccumulator

        rdd.foreach(num=>{
            // 使用累加器
            //sum = sum + num
            sum.add(num)
            //println("sum = " + sum)
        })

        // Driver
        // 访问累加器的值
        println("sum = " + sum.value)

        sc.stop()
    }
}
