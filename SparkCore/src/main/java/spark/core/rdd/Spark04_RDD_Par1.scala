package spark.core.rdd


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Par1 {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val list = List(1,2,3,4,5,6)

        // 如果想要改变分区，可以使用第二个参数来代替默认值
        val number: RDD[Int] = sc.makeRDD(list, 3)
        number.saveAsTextFile("./SparkCore/ouput")

        sc.stop
    }
}
