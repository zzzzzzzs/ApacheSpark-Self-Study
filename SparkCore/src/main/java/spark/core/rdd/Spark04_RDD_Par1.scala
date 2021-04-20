package spark.core.rdd


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Par1 {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 从内存中将数据保存成数据文件
        val list = List(1,2,3,4,5)

        // 如果想要改变分区，可以使用第二个参数来代替默认值
        // 这里每个分区放置的数据需要看源码，def positions(length: Long, numSlices: Int): Iterator[(Int, Int)]
        /**
         * 这里按照1，2，3，4，5举例子
         * 源码是按照numSlices做的迭代，比如这里是按照3切片
         * 0 => (0, 1) => 1
         * 1 => (1, 3) => 2,3
         * 2 => (3, 5) => 4,5
         */
        val number: RDD[Int] = sc.makeRDD(list, 3)
        number.saveAsTextFile("./SparkCore/output")

        sc.stop
    }
}
