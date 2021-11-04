package spark.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.elasticsearch.spark.rdd.EsSpark

/**
 * @author zs
 * @date 2021/11/4
 * 从es中将某个时间范围的数据查询出来放到hive中
 */
object Es2Hive {
  def main(args: Array[String]): Unit = {
    //    val starttime: String = (new mutable.StringBuilder).append(args(0)).append(" 00:00:00")
    //    val endtime: String = (new mutable.StringBuilder).append(args(1)).append(" 00:00:00")

    val starttime = "2021-11-02 00:00:00"
    val endtime = "2021-11-03 00:00:00"

    val conf = new SparkConf()
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("es.nodes", "http://es-cn-zz11ylgrr001x0fg5.public.elasticsearch.aliyuncs.com")
    conf.set("es.port", "9200")
    conf.set("es.net.http.auth.user", "elastic")
    conf.set("es.net.http.auth.pass", "RpBvUL=?643^eoJz7V!")
    conf.set("es.nodes.discovery", "false")
    conf.set("es.nodes.data.only", "false")
    conf.set("es.mapping.date.rich", "false")
    conf.set("es.scroll.size", "100")
    conf.set("es.scroll.keepalive", "10m")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark = SparkSession.builder().appName("EsToHive").master("local[*]").config(conf).enableHiveSupport().getOrCreate()
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

    val rdd = EsSpark.esRDD(spark.sparkContext, "flink-to-es-pro", query)

    val rdd1 = rdd.map(data => {
      val id = data._1.toString

      val cxt = data._2
      val from_wxid = cxt.get("from_wxid").get
      val to_wx_nick = cxt.get("to_wx_nick").get
      val file_path = Option(cxt.get("file_path").get).getOrElse("")
      val is_send = cxt.get("is_send").get.toString
      //      val file_name = Option(String.valueOf(cxt.get("file_name").get))
      //      val to_head_img = Option(String.valueOf(cxt.get("to_head_img").get))
      //      val channel_code = Option(String.valueOf(cxt.get("channel_code").get))
      //      val server_msg_id = Option(String.valueOf(cxt.get("server_msg_id").get))
      //      val media_uid = Option(String.valueOf(cxt.get("media_uid").get))
      //      val current_wxid = Option(String.valueOf(cxt.get("current_wxid").get))
      //      val to_wxid = Option(String.valueOf(cxt.get("to_wxid").get))
      //      val from_wx_nick = Option(String.valueOf(cxt.get("from_wx_nick").get))
      //      val sys_content = Option(String.valueOf(cxt.get("sys_content").get))
      //      val txt = Option(String.valueOf(cxt.get("txt").get))
      //      val detail_msg_type = Option(String.valueOf(cxt.get("detail_msg_type").get))
      //      val send_time = Option(String.valueOf(cxt.get("send_time").get))
      //      val send_timestamp = Option(String.valueOf(cxt.get("send_timestamp").get))
      //      val msg_type = Option(String.valueOf(cxt.get("msg_type").get))
      //      val server_log_path = Option(String.valueOf(cxt.get("server_log_path").get))
      //      val msg_md5 = Option(String.valueOf(cxt.get("msg_md5").get))
      //      val from_head_img = Option(String.valueOf(cxt.get("from_head_img").get))
      //      val server_rcv_time = Option(String.valueOf(cxt.get("server_rcv_time").get))
      Row(id, from_wxid, to_wx_nick,file_path,is_send)
      //      Row(id, from_wxid, to_wx_nick, file_path, is_send, file_name, to_head_img, channel_code, server_msg_id, media_uid, current_wxid, to_wxid, from_wx_nick, sys_content, txt, detail_msg_type, send_time, send_timestamp, msg_type, server_log_path, msg_md5, from_head_img, server_rcv_time)
    })

    val schema_data: StructType = StructType(
      List(
        // true代表不为空
        StructField("id", StringType),
        StructField("from_wxid", StringType),
        StructField("to_wx_nick", StringType),
        StructField("file_path", StringType),
        StructField("is_send", StringType)
        //        StructField("file_name", StringType),
        //        StructField("to_head_img", StringType),
        //        StructField("channel_code", StringType),
        //        StructField("server_msg_id", StringType),
        //        StructField("media_uid", StringType),
        //        StructField("current_wxid", StringType),
        //        StructField("to_wxid", StringType),
        //        StructField("from_wx_nick", StringType),
        //        StructField("sys_content", StringType)
        //        StructField("txt", StringType)
        //        StructField("detail_msg_type", StringType),
        //        StructField("send_time", StringType),
        //        StructField("send_timestamp", StringType),
        //        StructField("msg_type", StringType),
        //        StructField("server_log_path", StringType),
        //        StructField("msg_md5", StringType),
        //        StructField("from_head_img", StringType),
        //        StructField("server_rcv_time", StringType)
      )
    )
    spark.createDataFrame(rdd1, schema_data).createOrReplaceTempView("es_day")
//        spark.sql("insert overwrite table zmt_app_ods.ods_wechat_record_bak select id,channel_code,current_wxid,from_wxid,to_wxid,from_wx_nick,to_wx_nick,send_time,txt,sys_content,msg_type,file_name,file_path,server_rcv_time,server_log_path,media_uid,msg_md5,is_send,server_msg_id,detail_msg_type,send_timestamp,to_head_img,from_head_img from es_day")
//        spark.sql("select id,channel_code,current_wxid,from_wxid,to_wxid,from_wx_nick,to_wx_nick,send_time,txt,sys_content,msg_type,file_name,file_path,server_rcv_time,server_log_path,media_uid,msg_md5,is_send,server_msg_id,detail_msg_type,send_timestamp,to_head_img,from_head_img,substr(send_time,1,10) as day from es_day").show()
    spark.sql("select id,from_wxid, to_wx_nick,file_path,is_send from es_day").show()
    spark.stop

  }
}
