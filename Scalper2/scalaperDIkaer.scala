// /opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-shell --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 40g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096
//--deploy-mode cluster
package com.bl

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Set
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import java.util.Date

import com.bl.main.{emptyList, subGraph}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.storage.StorageLevel

object main {
 // Create the context
  val conf = new SparkConf().setAppName("FastUnfolding").set("spark.serializer","org.apache.spark.serializer.JavaSerializer")
  val sc = new SparkContext(conf)
  val sparkSql = SparkSession.builder.enableHiveSupport.getOrCreate()
  val sql = new SQLContext(sparkSql.sparkContext)

//会员笛卡尔积 方法1
  val member_allInfo=sparkSql.sql("select distinct member_id from algorithm_data.HnAllOrder_spark " ).cache()
  val member_allInfo_broadcast = sparkSql.sparkContext.broadcast(member_allInfo.collect())
  val list = member_allInfo_broadcast.value
  //双重循环，找出会员之间的关系值
  val member_allInfo_a=member_allInfo.rdd.mapPartitions(rows =>{
    rows.flatMap(Ylist=>{
      list.map(Xlist =>  {
        val memberPair = Set(Ylist(0),Xlist(0))
      })
    })
  }).cache()
  member_allInfo_broadcast.unpersist()

//会员笛卡尔积 方法2
  sql("select * from algorithm_data.HnAllOrder_spark").repartition(600).persist(StorageLevel.MEMORY_ONLY_2).createOrReplaceTempView("HnAllOrder")

  val rdd1 = sql("select * from HnAllOrder").rdd
  val rdd2 = rdd1

  val bc: Broadcast[RDD[Row]] = spark.sparkContext.broadcast(rdd1)
  rdd1.cartesian(bc.value)


//会员笛卡尔积 方法3
val member_allInfo=sparkSql.sql("select * from algorithm_data.HnAllOrder_spark limit 100" )
val member_allInfo_broadcast = sparkSql.sparkContext.broadcast(member_allInfo.collect())
val list = member_allInfo_broadcast.value

//list.length
//  list(1)
//将时间差<10认为是有关联的，>=10S认为是没有关联的
def getCoreTime(start_time:String,end_Time:String)={
  if (start_time== "999999"  || end_Time== "999999") {
    false
  }
  else {
    var df:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var begin:Date=df.parse(start_time)
    var end:Date = df.parse(end_Time)
    var between:Long=((end.getTime()-begin.getTime())/1000).abs  //转化成秒
    if (between<=10){
      true
    }else{
      false
    }
  }
}


def getMemberEdgeValue(Ylist:org.apache.spark.sql.Row,Xlist:org.apache.spark.sql.Row):Double= {
//  val memberPair = Set(Ylist(0),Xlist(0))
  var memberDim: List[Double] = List()
  if (Xlist(1)!= "999999"  && Ylist(1)==Xlist(1) ) {memberDim=memberDim:+0.5} else{memberDim=memberDim:+0.0} //"IP"
  if (Ylist(2)!= "999999"  && Ylist(2)==Xlist(2) ) {memberDim=memberDim:+2.0} else{memberDim=memberDim:+0.0}//"device_id"
  if (Ylist(3)!= "999999"  && Ylist(3)==Xlist(3) ) {memberDim=memberDim:+0.5} else{memberDim=memberDim:+0.0}//"recept_address_same"
  if (Ylist(4)!= "999999"  && Ylist(4)==Xlist(4) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_address_detail"
  if (Ylist(5)!= "999999"  && Ylist(5)==Xlist(5) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_name"
  if (Ylist(6)!= "999999"  && Ylist(6)==Xlist(6) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_phone"
  if (Ylist(7)!= "999999"  && Ylist(7)==Xlist(7) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"register_phone"
  //       if (Ylist(8)!= "999999"  && Ylist(8)==Xlist(8) ) {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"recept_mobilearea_a"
  //       if (Ylist(9)!= "999999"  && Ylist(9)==Xlist(9) ) {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"register_mobilearea_a"
  if (Ylist(10)!= "999999" && getCoreTime(Ylist(10).asInstanceOf[String],Xlist(10).asInstanceOf[String]) ) {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}//"register_time"
  if (Ylist(11)!= "999999" && getCoreTime(Ylist(11).asInstanceOf[String],Xlist(11).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"online_first_sale_time"
  if (Ylist(12)!= "999999" && getCoreTime(Ylist(12).asInstanceOf[String],Xlist(12).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"online_last_sale_time"
  //       if (Ylist(13)!= 999999.0 && (Ylist(13).asInstanceOf[Float]-Xlist(13).asInstanceOf[Float]).abs<10.0 )      {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0} //"online_first_need_amt" 第一次支付差价在10.0元之内
  //       if (Ylist(14)!= 999999.0 && (Ylist(14).asInstanceOf[Float]-Xlist(14).asInstanceOf[Float]).abs<10.0  )      {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"online_last_need_amt" 最后一次支付差价在10.0元之内
  if (Ylist(15)!= 999999.0 && (Ylist(15).asInstanceOf[Float]-Xlist(15).asInstanceOf[Float]).abs<10.0  )      {memberDim=memberDim:+0.3} else{memberDim=memberDim:+0.0}//"online_total_need_amt" 总共支付差价在10.0元之内
  if (Ylist(16)!= "999999" && getCoreTime(Ylist(16).asInstanceOf[String],Xlist(16).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0} //"first_acquire_time"
  if (Ylist(17)!= "999999" && getCoreTime(Ylist(17).asInstanceOf[String],Xlist(17).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"last_acquire_time"
  if (Ylist(18)!= 999999.0 && (Ylist(18).asInstanceOf[Float]-Xlist(18).asInstanceOf[Float]).abs < 10.0 && Xlist(18).asInstanceOf[Float]<=20 ) {memberDim=memberDim:+0.3} else{memberDim=memberDim:+0.0}//"getcouponsecdif"  领取优惠券和发放优惠券时间间隔在20min内，或者两个会员
  if (Ylist(19)!= 999999.0 && (Ylist(19).asInstanceOf[Float]-Xlist(19).asInstanceOf[Float]).abs < 10.0 )     {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}  //"orderdetailgoodscost"平均商品价格相差10.0元之内
  if (Ylist(20)!= 999999.0 && (Ylist(20).asInstanceOf[Float]-Xlist(20).asInstanceOf[Float]).abs < 10.0 )     {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}  //"avg_detailorder_discount" 订单折扣均价相差10.0元之内
  if (Ylist(21)!= 999999.0 && (Ylist(21).asInstanceOf[Float]-Xlist(21).asInstanceOf[Float]).abs<= 2  )     {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}  //"coupon_amount" 平均领券金额相差2元及以下
  if (Ylist(22)!= 999999.0 && (Ylist(22).asInstanceOf[Float]-Xlist(22).asInstanceOf[Float]).abs<=10.0  )     {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0} //"avgcouponuse" 每个商品平均折扣额 和 平均使用券额度之差，两个会员相差10.0元之内
  if (Ylist(23)!= 999999.0 && (Ylist(23).asInstanceOf[Float]-Xlist(23).asInstanceOf[Float]).abs<=2   )     {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}  //"getcouponnum"领取券的数量之差，在2个以内
  memberDim.sum
}

//      })
//    })
//  }).cache()





val member_allInfo_a=member_allInfo.rdd.flatMap(Ylist=>{
    list.map(Xlist =>  {
      val MemberEdgeValue=getMemberEdgeValue(Ylist,Xlist)
      (Ylist(0),Xlist(0),MemberEdgeValue)
    })
})

//member_allInfo_a.count()
//
//member_allInfo_broadcast.unpersist()


  val member_allInfo_b=member_allInfo_a.filter(x=>x._1!=x._2).filter(x=>x._3>2).map(x=>(Set(x._1,x._2),x._3))
//  val member_allInfo_c=member_allInfo_b.reduceByKey((x, y) =>if (x>y) {x} else{y})
  val member_allInfo_d=member_allInfo_b.map(x=>(x._1.toList,x._2))
  val member_allInfo_e=member_allInfo_d.map(x=>(x._1 :+ x._2))// 每个RDD是列表格式
  val member_allInfo_Edge=member_allInfo_e.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long],line(2).asInstanceOf[Double] )))   //EdgeRDD
  //2.2 vertic
//  val member_allInfo=sparkSql.sql("select * from algorithm_data.HnAllOrder_spark" ).cache()
  val member_allInfo_Ver =member_allInfo.rdd.map(x=>(x(0).asInstanceOf[Long],( (x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).asInstanceOf[Float],x(14).asInstanceOf[Float],x(15).asInstanceOf[Float],x(16).toString,x(17).toString,x(18).asInstanceOf[Float],x(19).asInstanceOf[Float],x(20).asInstanceOf[Float],x(21).asInstanceOf[Float]),(x(22).asInstanceOf[Float],x(23).asInstanceOf[Float]))))//VerticRDD
  //3.Graph
  val graph: Graph[( (String, String, String, String, String, String, String, String, String, String, String, String, Float, Float, Float, String, String, Float, Float, Float, Float), (Float, Float)), Double] = Graph(member_allInfo_Ver, member_allInfo_Edge)
  //4.connected Graph
  val connect_a: Graph[VertexId, Double]= graph.connectedComponents().cache()
  val connectFilted = connect_a.vertices.map(_.swap).groupByKey.filter(x => x._2.size > 5)







}