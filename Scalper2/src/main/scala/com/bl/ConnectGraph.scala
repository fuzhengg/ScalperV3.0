package com.bl

import java.text.SimpleDateFormat
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{graphx, sql}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}



class ConnectGraph extends java.io.Serializable{
  def timeMinuteDiff (date1:String,date2:String):Long={
    //返回相隔分钟数
    var diff:Long=999999
    if(date1=="999999" || date2=="999999" || date1=="" || date2==""){
      diff
    }else{
      val sdf =new SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
      val b=sdf.parse(date1).getTime-sdf.parse(date2).getTime
      diff=b/(1000*60)
    }
    diff.abs
  }

  def getNewProperty(srcAttr:List[String],dstAttr:List[String] ,attr: Double):(Double, Double) = {
    var Score = attr
    //根据人组合和消费特征组合，目的：0.9+A=0.15+A+B
    //消费特征A=0.2
    if (timeMinuteDiff(srcAttr(18).toString, dstAttr(18).toString) < 5) Score += 0.1 else Score //最后一次销售时间相差10Min之内
    if ((srcAttr(19).toDouble - dstAttr(19).toDouble).abs < 10   &&  srcAttr(19) != "999999"  ) Score += 0.05 else Score //最后一次销售金额相差小于10元
    //消费特征B =0.75
    if (timeMinuteDiff(srcAttr(0).toString, dstAttr(0).toString) < 5) Score += 0.15 else Score //注册时间相差5Min之内
    if (timeMinuteDiff(srcAttr(22).toString, dstAttr(22).toString) < 5) Score += 0.1 else Score //第一次销售时间相差10Min之内
    if ((srcAttr(23).toDouble - dstAttr(23).toDouble).abs < 10 && srcAttr(23) != "999999") Score += 0.05 else Score //第一次销售金额相差小于10元
    if ((srcAttr(26).toDouble - dstAttr(26).toDouble).abs < 10  && srcAttr(26) != "999999") Score += 0.15 else Score //总销售金额相差小于10元
    if ((srcAttr(27).toDouble - dstAttr(27).toDouble).abs < 10  && srcAttr(27) != "999999") Score += 0.15 else Score //总销售折扣金额相差小于5元
    if (timeMinuteDiff(srcAttr(30).toString, dstAttr(30).toString) < 5) Score += 0.05 else Score //最后一次领取优惠券时间相差10Min之内
    //消费特征C=0.06
    if ((srcAttr(20).toDouble - dstAttr(20).toDouble).abs < 5  && srcAttr(20) != "999999"  )   Score += 0.02 else Score //最后一次销售折扣金额相差小于5元
    if ((srcAttr(24).toDouble - dstAttr(24).toDouble).abs < 5  && srcAttr(24) != "999999") Score += 0.01 else Score //第一次销售折扣金额相差小于5元
    if ((srcAttr(29).toDouble - dstAttr(29).toDouble).abs < 3   && srcAttr(29) != "999999") Score += 0.02 else Score //领取优惠券数量相差小于3
    if ((srcAttr(31).toDouble - dstAttr(31).toDouble).abs < 1   && srcAttr(31) != "999999") Score += 0.01 else Score //最后一次领取优惠券金额相差小于5元
    (attr, Score)
  }


  def getConnectGraph(sparkSql:SparkSession,all_dim:org.apache.spark.rdd.RDD[(scala.collection.immutable.Set[Long], Double)],HnMember_info:sql.DataFrame): org.apache.spark.sql.DataFrame= {
    //1. edge,vertic  for Graph
    //1.1 edge
    val crossMemberSim=all_dim
    val member_allInfo_b=crossMemberSim.filter(x=>x._2 >=0.15)
    val member_allInfo_c=member_allInfo_b.reduceByKey((x, y) =>if (x>y) {x} else{y})
    val member_allInfo_d=member_allInfo_c.map(x=>(x._1.toList,x._2)).filter(x=>x._1.size>=0.6)
    val member_allInfo_e=member_allInfo_d.map(x=>(x._1 :+ x._2))// 每个RDD是列表格式
    val member_allInfo_Edge : RDD[Edge[Double]] =member_allInfo_e.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long],line(2).asInstanceOf[Double] )))   //EdgeRDD
    //1.2 vertic
    val member_allInfo_Ver: RDD[(VertexId, List[String])]=HnMember_info.rdd.map(x=>( x(0).toString.toLong, List(x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).toString,x(14).toString,x(15).toString,x(16).toString,x(17).toString,x(18).toString,x(19).toString,x(20).toString,x(21).toString,x(22).toString,x(23).toString,x(24).toString,x(25).toString,x(26).toString,x(27).toString,x(28).toString,x(29).toString,x(30).toString,x(31).toString,x(32).toString,x(33).toString,x(34).toString,x(35).toString,x(36).toString,x(37).toString,x(38).toString,x(39).toString,x(40).toString,x(41).toString,x(42).toString)))//VerticRDD

    //2.Graph
    var propertySet = List("999999")
    val graph_a=Graph.fromEdges(member_allInfo_Edge, propertySet)
    val graph_b = graph_a.joinVertices(member_allInfo_Ver)((vid, attr, deps)=>deps)
    val graph_c = graph_b.mapTriplets(x=>getNewProperty(x.srcAttr,x.dstAttr,x.attr))

//    graph_d.edges.map(x=>(x.attr._2.toInt,x.srcId)).distinct().countByKey()
//    graph_d.vertices.first
    val graph_d = graph_c.subgraph( epred = edge => edge.attr._2 > 0.7)  //新的边属性值要大于0.65
    val graph_e = graph_d.subgraph( epred = edge => edge.attr._2 -edge.attr._1>0.1)  //商品属性必须有一个属性匹配

    //3.connected Graph
    val connect_a = graph_e.connectedComponents().cache()


    //   通过PageRank可以找出最关键的人
//    val pagerankGraph = graph_d.pageRank(0.001)
//    val titleAndPrGraph = graph_d.outerJoinVertices(pagerankGraph.vertices) {
//      (v, title, rank) => (rank.getOrElse(0.0), title)
//    }
//    titleAndPrGraph.vertices.top(10) {
//      Ordering.by((entry: (VertexId, (Double, List[String]))) => entry._2._1)
//    }.foreach(t => println(t._2._2 + ": " + t._2._1))


//    connect_a.vertices.first()
    System.out.println("社区模型计算结果过滤")
    val connectFilted =connect_a.degrees.filter(x=>x._2>5)//度大于8
    //社交圈结果,且社交圈会员数量大于8
    val connectFilted_a=connect_a.vertices.join(connectFilted).map(x=>Row(x._2._1.toLong,x._1.toLong))

    val connectFiltedSchema = StructType(
      Seq(
        StructField("circle_id",LongType,true)
        ,StructField("circle_memberID",LongType,true)
      )
    )
    val connectFilted_aDf=sparkSql.createDataFrame(connectFilted_a,connectFiltedSchema)
    connectFilted_aDf.createOrReplaceTempView("connectFiltedATable")
    val connectFiltedTableDf=sparkSql.sql("select  a.circle_id,a.circle_size,b.circle_memberID " +
                                     "from (select circle_id,count(distinct circle_memberID) circle_size from connectFiltedATable group by circle_id) a  " +
                                     "join connectFiltedATable b on a.circle_id=b.circle_id " +
                                     "where a.circle_size>8 ")
    //connectFiltedTableDf.select("circle_memberID").distinct().count()
//    sparkSql.sql("select * from connectFiltedATable where circle_memberID IN ('100000008809931','100000008809913','100000008809920')").collect()
    connectFiltedTableDf
  }
}
