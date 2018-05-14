// /opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-shell --executor-cores 6 --num-executors 20 --executor-memory 7g --master yarn  --driver-memory 10g --name Scalper
// --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096
//--deploy-mode cluster
//理论上该版本和上版本好很多，改进为：
//1. 修改生成概率转移矩阵维度，使得下单信息和收货信息结合，避免多人给一人送礼造成的误杀
//2. 连通图尺寸问题得以修正
//3. 朋友圈大于8个人才会被拦截


package com.bl

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object ScalperThird extends java.io.Serializable  {
  def main(args: Array[String]): Unit = {
    //Prepare
    val Year = strToInt(args(0))
    val Month = strToInt(args(1))
    val Day = strToInt(args(2))
    val cg = "20180322nonRealTime"

    //1. Create the context
    System.out.println("黄牛模型开始计算")
    val conf = new SparkConf().setAppName("ScalperThird").set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val sc = new SparkContext(conf)
    val sparkSql = SparkSession.builder.enableHiveSupport.getOrCreate()
    val HnMember_info = sparkSql.sql(s"SELECT a.* " +
                                       s"FROM  algorithm_data.HnMember_info a  " +
                                       s"where  ecp_ind is not null and online_total_need_amt is not null and member_id is not null ")
    //    HnMember_info.printSchema()
    //2. memberCross Info
    System.out.println("会员交叉比较")
    val mc = new memberCross()
    val all_dim = mc.getMemberCross(sparkSql,HnMember_info)

    //3. ConnectGraph
    System.out.println("社区模型计算")
    val cgraph = new ConnectGraph()
    val ConnectGraph = cgraph.getConnectGraph(sparkSql,all_dim,HnMember_info)
    HnMember_info.unpersist()

    //4.mark Circle and save the result to Table
    System.out.println("社交圈打分,保存模型计算结果")
    val save = new saveCircle()
    save.circleScore(Year,Month,Day,cg,sparkSql,ConnectGraph,HnMember_info)


















    //      //6.logistic regression
    //      //6.1 all vertic of ConnectGraph
    //      val cc=ConnectGraph.vertices.filter(x=>(x._1!=x._2))  //所有顶点
    //      //6.2. newVertic Infomation
    //      val emptyList: List[Any] = List()
    //      val users =sparkSql.sql("select * " +
    //        "from algorithm_data.HnAllOrder_spark_LG " +
    //        "where register_time IS NOT NULL " +
    //        "AND  online_last_sale_time IS NOT NULL " +
    //        "AND  online_first_sale_time IS NOT NULL").rdd
    //      val users_Ver: RDD[ (VertexId, List[Any]) ]=users.map(x => (x(0).asInstanceOf[Long],List(x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long],x(4).asInstanceOf[Long],x(5).asInstanceOf[Long],x(6).asInstanceOf[Double],x(7).asInstanceOf[Double],x(8).asInstanceOf[Double],x(9).asInstanceOf[Double],x(10).asInstanceOf[Double],x(11).asInstanceOf[Double],x(12).asInstanceOf[Double],x(13).asInstanceOf[Double]) ))
    //      //6.3 new vertics of  ConnectGraph
    //      val ValidConnectAttr=users_Ver.join(cc).map{case(id,(attr,circleLabel))=>(circleLabel,attr)}  //加入新顶点信息
    //      //6.4. get circleDim
    //      case class circleDimSchema(circle_id:Long,member_level_id:Double,ecp_ind:Double,register_time:Double,online_last_sale_time:Double,online_first_sale_time:Double,online_first_sale_amt:Double,online_first_discount_amt:Double,online_last_sale_amt:Double,online_last_discount_amt:Double,online_total_sale_amt:Double,online_total_discount_amt:Double,getcouponsecdifavg:Double,getcouponnum:Double)
    //      val circleDimDf=ValidConnectAttr.map(x=>(x._1.asInstanceOf[Long],x._2(0).asInstanceOf[Double],x._2(1).asInstanceOf[Double],x._2(2).asInstanceOf[Double],x._2(3).asInstanceOf[Double],x._2(4).asInstanceOf[Double],x._2(5).asInstanceOf[Double],x._2(6).asInstanceOf[Double],x._2(7).asInstanceOf[Double],x._2(8).asInstanceOf[Double],x._2(9).asInstanceOf[Double],x._2(10).asInstanceOf[Double],x._2(11).asInstanceOf[Double],x._2(12).asInstanceOf[Double]))
    //      val circleDimTableDf=circleDimDf.map(x=>circleDimSchema(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14)).toDF()
    //      circleDimTableDf.createOrReplaceTempView("circleDimTable")
    //      //6.5 logistic
  }
//      * 判断字符串是否是纯数字组成的串，如果是，就返回对应的数值，否则返回0
  def strToInt(str: String): Int = {
    val regex = """([0-9]+)""".r
    val res = str match{
      case regex(num) => num
      case _ => "999"
    }
    val resInt = Integer.parseInt(res)
    resInt
  }
}