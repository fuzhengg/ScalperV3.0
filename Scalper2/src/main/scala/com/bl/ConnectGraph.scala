package com.bl


import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Set

object ConnectGraph extends java.io.Serializable{
  def getConnectGraph(sparkSql:SparkSession): Graph[VertexId, Double]= {
    //1. member relation
    val crossMemberSim = sparkSql.sql("select  * from algorithm_data.hnallorder_spark_crossInfo")
    //2. edge,vertic  for Graph
    //2.1 edge
    val member_allInfo_a  =crossMemberSim.rdd.map(x=>(Set(x(0),x(1)),List(java.lang.Double.parseDouble(x(2).toString),java.lang.Double.parseDouble(x(3).toString),java.lang.Double.parseDouble(x(4).toString),java.lang.Double.parseDouble(x(5).toString),java.lang.Double.parseDouble(x(6).toString),java.lang.Double.parseDouble(x(7).toString),java.lang.Double.parseDouble(x(8).toString),java.lang.Double.parseDouble(x(9).toString),java.lang.Double.parseDouble(x(10).toString),java.lang.Double.parseDouble(x(11).toString),java.lang.Double.parseDouble(x(12).toString),java.lang.Double.parseDouble(x(13).toString),java.lang.Double.parseDouble(x(14).toString),java.lang.Double.parseDouble(x(15).toString),java.lang.Double.parseDouble(x(16).toString),java.lang.Double.parseDouble(x(17).toString),java.lang.Double.parseDouble(x(18).toString),java.lang.Double.parseDouble(x(19).toString),java.lang.Double.parseDouble(x(20).toString),java.lang.Double.parseDouble(x(21).toString),java.lang.Double.parseDouble(x(22).toString))))
    val member_allInfo_b=member_allInfo_a.filter(x=>x._1.size>1).map(x=>(x._1,x._2.sum)).filter(x=>x._2>2)
    val member_allInfo_c=member_allInfo_b.reduceByKey((x, y) =>if (x>y) {x} else{y})
    val member_allInfo_d=member_allInfo_c.map(x=>(x._1.toList,x._2))
    val member_allInfo_e=member_allInfo_d.map(x=>(x._1 :+ x._2)).filter(x=>x(0)!=x(1))// 每个RDD是列表格式
    val member_allInfo_Edge=member_allInfo_e.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long],line(2).asInstanceOf[Double] )))   //EdgeRDD
    //2.2 vertic
    val member_allInfo=sparkSql.sql("select * from algorithm_data.HnAllOrder_spark" ).cache()
    val member_allInfo_Ver =member_allInfo.rdd.map(x=>(x(0).asInstanceOf[Long],( (x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).asInstanceOf[Float],x(14).asInstanceOf[Float],x(15).asInstanceOf[Float],x(16).toString,x(17).toString,x(18).asInstanceOf[Float],x(19).asInstanceOf[Float],x(20).asInstanceOf[Float],x(21).asInstanceOf[Float]),(x(22).asInstanceOf[Float],x(23).asInstanceOf[Float]))))//VerticRDD
    //3.Graph
    val graph: Graph[( (String, String, String, String, String, String, String, String, String, String, String, String, Float, Float, Float, String, String, Float, Float, Float, Float), (Float, Float)), Double] = Graph(member_allInfo_Ver, member_allInfo_Edge)
    //4.connected Graph
    val connect_a: Graph[VertexId, Double]= graph.connectedComponents().cache()
    connect_a
  }
}
