package spark.core.rdd


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Par {

    def main(args: Array[String]): Unit = {

        // local[4] 表示在本地的4个CPU核上运行
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        // TODO Spark -  RDD的分区和并行度

        // RDD的分区主要用于分布式计算，可以将数据发送到不同的Executor执行计算。和并行有关系
        // 并行度表示在整个集群执行时，同时执行任务的数量
        // 分区的数量和并行度其实没有直接的关系。并行度主要取决于CPU核的数量


        // TODO 从内存中将数据保存成文件
        // RDD的分区数量是可以在创建时更改的。如果不更改，那么使用默认的分区
        //        val list = List(1,2,3,4,5,6,7,8)
        val list = List(1, 2, 3, 4, 5)
        // makeRDD方法的第二个参数就是默认分区的数量
        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // scheduler.conf = SparkConf
        // spark.default.parallelism ： Spark默认并行度
        // 如果从配置信息中获取不到spark.default.parallelism参数，那么会使用默认值totalCores
        // totalCores表示当前环境配置的总核数
        val number: RDD[Int] = sc.makeRDD(list, 8)
        // 将RDD保存成分区文件
        // 8个 => 默认有8个分区 => 环境中总核数
        number.saveAsTextFile("./SparkCore/output")

        sc.stop
    }
}
