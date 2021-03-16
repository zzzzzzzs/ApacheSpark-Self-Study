package spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Ser1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        // TODO Spark - 序列化

        val list = List()
        val rdd = sc.makeRDD(list)

        // Driver
        val user = new User()

        // Task not serializable
        // NotSerializableException: com.atguigu.bigdata.spark.core.serial.Spark02_Ser1$User

        // 如果Executor端使用了Driver端的数据，那么这个数据需要序列化，否则无法在网络中传递。

        // 闭包检测是执行作业的前提条件。如果检测失败，那么作业根本无法执行
        rdd.foreach(
            num => {
                // Executor
                println(user)
            }
        )

        sc.stop()
    }
    class User {

    }
}
