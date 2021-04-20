package spark.core.rdd


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_File_Par2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 文件数据的分区
        // textFile可以将文件作为数据处理的数据源，默认也可以设定分区
        // minPartitions：最小分区数量
        // 读取文件时defaultMinPartitions默认分区数量为 ：
        //              上下文环境local[*]默认分区是按照计算机的核数设定的
        //              math.min(defaultParallelism, 2) => 2
        //              由于默认是最小分区，所以是2
        // textFile的第二个参数表示最小分区的数量(不见得相等)
        // defaultParallelism = scheduler.conf.getInt("spark.default.parallelism", totalCores)
        val file: RDD[String] = sc.textFile("SparkCore/datas/1.txt")

        file.saveAsTextFile("./SparkCore/output")

        sc.stop
    }
}
