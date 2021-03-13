package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Par6 {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        /*

           totalsize : 17
           num : 3
           goalsize : 17 / 3 = 5...2

           partition cnt = num + 1 = 3 + 1 => 4


            1@@   =>0, 1, 2
            23@@  =>3, 4, 5, 6
            456@@ =>7, 8, 9, 10, 11
            78910 =>12, 13, 14, 15, 16


           // 0 - 5   => [0, 5]    => 【1, 23】
           // 5 - 5   => [5, 10]   => 【456】
           // 10 - 5  => [10, 15]  => 【78910】
           // 15 - 2  => [15, 17]  => 【】

         */

        val rdd3: RDD[String] = sc.textFile("input/a.txt", 3)

        /*
           totalsize : 17
           num : 4
           goalsize : 17 / 4 = 4...1
           partition cnt = num + 1 = 4 + 1 => 5

            1@@   =>0, 1, 2
            23@@  =>3, 4, 5, 6
            456@@ =>7, 8, 9, 10, 11
            78910 =>12, 13, 14, 15, 16

            0 + 4  => [0, 4]    => 【1，23】
            4 + 4  => [4, 8]    => 【456】
            8 + 4  => [8, 12]   => 【78910】
            12 + 4 => [12, 16]  => 【】
            16 + 1 => [16, 17]  => 【】
         */



        val rdd4: RDD[String] = sc.textFile("input/a.txt", 4)
        rdd3.saveAsTextFile("output3")
        rdd4.saveAsTextFile("output4")

        sc.stop
    }
}
