package spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * spark-使用十种[算子]实现wordcount
 */
object Spark03_WordCount10Way {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
        val sc = new SparkContext(sparkConf)
        //    val rdd = sc.textFile("input/wc.txt").flatMap(datas => {
        //      datas.split(" ")
        //    })
        val rdd = sc.makeRDD(List("hadoop", "hello", "spark", "hello", "scala", "hello", "scala", "spark"))

        println("=================1====================")

        rdd.countByValue().foreach(println)

        println("=================2====================")

        rdd.map((_, 1)).countByKey().foreach(println)

        println("=================3====================")

        rdd.map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

        println("=================4====================")

        rdd.map((_, 1)).groupByKey().mapValues(_.size).collect().foreach(println)

        println("=================5====================")

        rdd.map((_, 1)).aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

        println("=================6====================")

        rdd.map((_, 1)).foldByKey(0)(_ + _).collect().foreach(println)

        println("=================7====================")

        rdd.map((_, 1)).combineByKey(
            (num: Int) => num,
            (x: Int, y: Int) => {
                x + y
            },
            (x: Int, y: Int) => {
                x + y
            }
        ).collect().foreach(println)

        println("=================8====================")

        rdd.map((_, 1)).groupBy(_._1).map(kv => {
            (kv._1, kv._2.size)
        }).collect().foreach(println)

        println("=================9====================")

        rdd.aggregate(mutable.Map[String, Int]())(
            (map, word) => {
                map(word) = map.getOrElse(word, 0) + 1
                map
            },
            (map1, map2) => {
                map1.foldLeft(map2)(
                    (finalMap, kv) => {
                        finalMap(kv._1) = finalMap.getOrElse(kv._1, 0) + kv._2
                        finalMap
                    }
                )
            }
        ).foreach(println)

        println("=================10====================")

        rdd.map(s => mutable.Map(s -> 1)).fold(mutable.Map[String, Int]())(
            (map1, map2) => {
                map1.foldLeft(map2)(
                    (finalMap, kv) => {
                        finalMap(kv._1) = finalMap.getOrElse(kv._1, 0) + kv._2
                        finalMap
                    }
                )
            }
        ).foreach(println)

        sc.stop()
    }
}
