/**
 * @author zs
 * @date 2021/11/4
 */
object StringTest {
  def main(args: Array[String]): Unit = {
    val starttime = "2021"
    val endtime = "2021"
    val query =
    """
      |{
      |  "query":{
      |    "range": {
      |      "send_time": {
      |        "gte": "&",
      |        "lte": "@"
      |      }
      |    }
      |  }
      |}
    """.stripMargin.replace("&", starttime).replace("@", endtime)

    println(query)
  }

}
