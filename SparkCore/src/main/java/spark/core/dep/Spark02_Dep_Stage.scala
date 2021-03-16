package spark.core.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Dep_Stage {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // RDD中阶段的划分
        // DAGScheduler.handleJobSubmitted
        // 有向无环图调度器对象中完成了整个作业的阶段的划分
        // 前面的阶段不执行完，是不能执行后续阶段。

        // TODO L986 : finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
        // 1. 如果执行过程中，没有shuffle操作，那么至少也有一个阶段：ResultStage
        //     new ResultStage

        // 2. 创建ResultStage之前，执行了
        //    val parents = getOrCreateParentStages(rdd, jobId)

        // 3. 获取shuffle依赖（宽依赖）集合后进行了转换
        //    shuffleDep => new ShuffleMapStage
        /*
            getShuffleDependencies(rdd).map { shuffleDep =>
              getOrCreateShuffleMapStage(shuffleDep, firstJobId)
            }.toList
         */

        // 4. 如何获取当前RDD的依赖关系（宽依赖）
        /*

  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    parents
  }

         */

        // TODO Spark作业中阶段的数量等于 ： 1 + shuffle依赖的个数

        sc.stop()

    }
}
