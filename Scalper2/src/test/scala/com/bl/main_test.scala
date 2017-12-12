//// /opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-shell --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 30g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096
////--deploy-mode cluster
//package com.bl
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark.graphx.{Edge, _}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.{ArrayBuffer, Set}
//
//object main {
//  //1. Create the context
//  val conf = new SparkConf().setAppName("FastUnfolding").set("spark.serializer","org.apache.spark.serializer.JavaSerializer")
//  val sc = new SparkContext(conf)
//  val sparkSql = SparkSession.builder.enableHiveSupport.getOrCreate()
//  val member_allInfo=sparkSql.sql("select * from algorithm_data.HnAllOrder_spark where year(register_time)=2017 and month(register_time)=11" ).coalesce(300).cache()
//
//  val member_allInfo_broadcast = sparkSql.sparkContext.broadcast(member_allInfo.collect())
//  val list = member_allInfo_broadcast.value
//  member_allInfo_broadcast.unpersist()
////  list.length
//  //    member_allInfo.printSchema()
//  //  val member_allInfo_len=member_allInfo.map(x=>x(0).asInstanceOf[Long]).distinct().count()
//  //------------------------------------------------------------------------------------------------
//  //将时间差<10认为是有关联的，>=10S认为是没有关联的
//  def getCoreTime(start_time:String,end_Time:String)={
//    if (start_time== "999999"  || end_Time== "999999") {
//      false
//    }
//    else {
//      var df:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      var begin:Date=df.parse(start_time)
//      var end:Date = df.parse(end_Time)
//      var between:Long=((end.getTime()-begin.getTime())/1000).abs  //转化成秒
//      if (between<=10){
//        true
//      }else{
//        false
//      }
//    }
//  }
//
//  //双重循环，找出会员之间的关系值
//  val member_allInfo_a=member_allInfo.rdd.mapPartitions(rows =>{
//    rows.flatMap(Ylist=>{
//      list.map(Xlist =>  {
//        val memberPair = Set(Ylist(0),Xlist(0))
//        var memberDim: List[Double] = List()
//        //最大：30
//        //+3 device_id,online_total_need_amt
//        //+2 IP,recept_phone,recept_name,recept_address_detail，orderdetailgoodscost，avg_detailorder_discount，avgcouponuse,getcouponsecdif
//        //+1 others
//        if (Xlist(1)!= "999999"  && Ylist(1)==Xlist(1) ) {memberDim=memberDim:+0.5} else{memberDim=memberDim:+0.0} //"IP"
//        if (Ylist(2)!= "999999"  && Ylist(2)==Xlist(2) ) {memberDim=memberDim:+2.0} else{memberDim=memberDim:+0.0}//"device_id"
//        if (Ylist(3)!= "999999"  && Ylist(3)==Xlist(3) ) {memberDim=memberDim:+0.5} else{memberDim=memberDim:+0.0}//"recept_address_same"
//        if (Ylist(4)!= "999999"  && Ylist(4)==Xlist(4) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_address_detail"
//        if (Ylist(5)!= "999999"  && Ylist(5)==Xlist(5) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_name"
//        if (Ylist(6)!= "999999"  && Ylist(6)==Xlist(6) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"recept_phone"
//        if (Ylist(7)!= "999999"  && Ylist(7)==Xlist(7) ) {memberDim=memberDim:+1.0} else{memberDim=memberDim:+0.0}//"register_phone"
////        if (Ylist(8)!= "999999"  && Ylist(8)==Xlist(8) ) {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"recept_mobilearea_a"
////        if (Ylist(9)!= "999999"  && Ylist(9)==Xlist(9) ) {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"register_mobilearea_a"
//        if (Ylist(10)!= "999999" && getCoreTime(Ylist(10).asInstanceOf[String],Xlist(10).asInstanceOf[String]) ) {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}//"register_time"
//        if (Ylist(11)!= "999999" && getCoreTime(Ylist(11).asInstanceOf[String],Xlist(11).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"online_first_sale_time"
//        if (Ylist(12)!= "999999" && getCoreTime(Ylist(12).asInstanceOf[String],Xlist(12).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"online_last_sale_time"
////        if (Ylist(13)!= 999999.0 && (Ylist(13).asInstanceOf[Float]-Xlist(13).asInstanceOf[Float]).abs<10.0 )      {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0} //"online_first_need_amt" 第一次支付差价在10.0元之内
////        if (Ylist(14)!= 999999.0 && (Ylist(14).asInstanceOf[Float]-Xlist(14).asInstanceOf[Float]).abs<10.0  )      {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}//"online_last_need_amt" 最后一次支付差价在10.0元之内
//        if (Ylist(15)!= 999999.0 && (Ylist(15).asInstanceOf[Float]-Xlist(15).asInstanceOf[Float]).abs<10.0  )      {memberDim=memberDim:+0.3} else{memberDim=memberDim:+0.0}//"online_total_need_amt" 总共支付差价在10.0元之内
//        if (Ylist(16)!= "999999" && getCoreTime(Ylist(16).asInstanceOf[String],Xlist(16).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0} //"first_acquire_time"
//        if (Ylist(17)!= "999999" && getCoreTime(Ylist(17).asInstanceOf[String],Xlist(17).asInstanceOf[String]) ) {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0}//"last_acquire_time"
//
//        if (Ylist(18)!= 999999.0 && (Ylist(18).asInstanceOf[Float]-Xlist(18).asInstanceOf[Float]).abs < 10.0 && Xlist(18).asInstanceOf[Float]<=20 ) {memberDim=memberDim:+0.3} else{memberDim=memberDim:+0.0}//"getcouponsecdif"  领取优惠券和发放优惠券时间间隔在20min内，或者两个会员
//        if (Ylist(19)!= 999999.0 && (Ylist(19).asInstanceOf[Float]-Xlist(19).asInstanceOf[Float]).abs < 10.0 )     {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}  //"orderdetailgoodscost"平均商品价格相差10.0元之内
//        if (Ylist(20)!= 999999.0 && (Ylist(20).asInstanceOf[Float]-Xlist(20).asInstanceOf[Float]).abs < 10.0 )     {memberDim=memberDim:+0.2} else{memberDim=memberDim:+0.0}  //"avg_detailorder_discount" 订单折扣均价相差10.0元之内
//        if (Ylist(21)!= 999999.0 && (Ylist(21).asInstanceOf[Float]-Xlist(21).asInstanceOf[Float]).abs<= 2  )     {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}  //"coupon_amount" 平均领券金额相差2元及以下
//        if (Ylist(22)!= 999999.0 && (Ylist(22).asInstanceOf[Float]-Xlist(22).asInstanceOf[Float]).abs<=10.0  )     {memberDim=memberDim:+0.1} else{memberDim=memberDim:+0.0} //"avgcouponuse" 每个商品平均折扣额 和 平均使用券额度之差，两个会员相差10.0元之内
//        if (Ylist(23)!= 999999.0 && (Ylist(23).asInstanceOf[Float]-Xlist(23).asInstanceOf[Float]).abs<=2   )     {memberDim=memberDim:+0.0} else{memberDim=memberDim:+0.0}  //"getcouponnum"领取券的数量之差，在2个以内
//        (memberPair,memberDim)
//      })
//    })
//  }).cache()
//
//
////一个会员也许会员多条记录，造成与其他会员的关系值得重复，在这种情况下取最大值(排除关系小于1.5的)
//  val member_allInfo_b=member_allInfo_a.filter(x=>x._1.size>1).map(x=>(x._1,x._2.sum)).filter(x=>x._2>2)
//  val member_allInfo_c=member_allInfo_b.reduceByKey((x, y) =>if (x>y) {x} else{y})
//  val member_allInfo_d=member_allInfo_c.map(x=>(x._1.toList,x._2))
//  val member_allInfo_e=member_allInfo_d.map(x=>(x._1 :+ x._2)).filter(x=>x(0)!=x(1))// 每个RDD是列表格式
//  val member_allInfo_Edge=member_allInfo_e.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long],line(2).asInstanceOf[Double] ))) //构建EdgeRDD
//  //  val member_allInfo_Edge=member_allInfo_e.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long] ))) //构建EdgeRDD
//  val member_allInfo_Ver =member_allInfo.rdd.map(x=>(x(0).asInstanceOf[Long],( (x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).asInstanceOf[Float],x(14).asInstanceOf[Float],x(15).asInstanceOf[Float],x(16).toString,x(17).toString,x(18).asInstanceOf[Float],x(19).asInstanceOf[Float],x(20).asInstanceOf[Float],x(21).asInstanceOf[Float]),(x(22).asInstanceOf[Float],x(23).asInstanceOf[Float]))))
//
//  //构造图
//  val graph: Graph[( (String, String, String, String, String, String, String, String, String, String, String, String, Float, Float, Float, String, String, Float, Float, Float, Float), (Float, Float)), Double] = Graph(member_allInfo_Ver, member_allInfo_Edge)
//  //  connect_a.vertices.collect.foreach(println(_))
//  //  ValidConnect.edges.collect.foreach(println(_))
//  //  连通图
//  val connect_a: Graph[VertexId, Double]= graph.connectedComponents().cache()
//  member_allInfo.unpersist()
//  val connect_b=connect_a.vertices.map(_.swap).groupByKey.filter(x=>x._2.size>9)  //社交圈结果,且社交圈会员数量大于5
//  val cc=connect_a.vertices.filter(x=>(x._1!=x._2))  //所有定点
//
//  val testvalidConnect = connect_b.map(x=>(x._1,x._2,x._2.size))
//  testvalidConnect.saveAsTextFile("hdfs:///user/fuzheng/aa.txt")
//
//  //新顶点信息
//  val emptyList: List[Any] = List()
//  val users =sparkSql.sql("select * from algorithm_data.HnAllOrder_spark_LG where register_time IS NOT NULL AND  online_last_sale_time IS NOT NULL AND  online_first_sale_time IS NOT NULL").rdd
//  val users_Ver: RDD[ (VertexId, List[Any]) ]=users.map(x => (x(0).asInstanceOf[Long],List(x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long],x(4).asInstanceOf[Long],x(5).asInstanceOf[Long],x(6).asInstanceOf[Double],x(7).asInstanceOf[Double],x(8).asInstanceOf[Double],x(9).asInstanceOf[Double],x(10).asInstanceOf[Double],x(11).asInstanceOf[Double],x(12).asInstanceOf[Double],x(13).asInstanceOf[Double]) ))
//  val ValidConnectAttr=users_Ver.join(cc).map{case(id,(attr,circleLabel))=>(circleLabel,attr)}  //加入顶点信息
//
//
//  //不同的社交圈求其对应的社交圈属性唯一值
//  case class circleDimSchema(circle_id:Long,member_level_id:Double,ecp_ind:Double,register_time:Double,online_last_sale_time:Double,online_first_sale_time:Double,online_first_sale_amt:Double,online_first_discount_amt:Double,online_last_sale_amt:Double,online_last_discount_amt:Double,online_total_sale_amt:Double,online_total_discount_amt:Double,getcouponsecdifavg:Double,getcouponnum:Double)
//  val circleDimDf=ValidConnectAttr.map(x=>(x._1.asInstanceOf[Long],x._2(0).asInstanceOf[Double],x._2(1).asInstanceOf[Double],x._2(2).asInstanceOf[Double],x._2(3).asInstanceOf[Double],x._2(4).asInstanceOf[Double],x._2(5).asInstanceOf[Double],x._2(6).asInstanceOf[Double],x._2(7).asInstanceOf[Double],x._2(8).asInstanceOf[Double],x._2(9).asInstanceOf[Double],x._2(10).asInstanceOf[Double],x._2(11).asInstanceOf[Double],x._2(12).asInstanceOf[Double]))
//  val circleDimTableDf=circleDimDf.map(x=>circleDimSchema(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14)).toDF()
//  circleDimTableDf.registerTempTable("circleDimTable")
////  val circleDim=sparkSql.sql("select circle_id" +
////                             ",variance(member_level_id) member_level_idVar,variance(ecp_ind) ecp_indVar" +
////                             ",variance(register_time) register_timeVar,variance(online_last_sale_time) online_last_sale_timeVar" +
////                             ",variance(online_first_sale_time) online_first_sale_timeVar,variance(online_first_sale_amt) online_first_sale_amtVar" +
////                             ",variance(online_first_discount_amt) online_first_discount_amtVar,variance(online_last_sale_amt) online_last_sale_amtVar" +
////                             ",variance(online_last_discount_amt) online_last_discount_amtVar,variance(online_total_sale_amt) online_total_sale_amtVar" +
////                             ",variance(online_total_discount_amt) online_total_discount_amtVar,variance(getcouponsecdifavg) getcouponsecdifavgVar" +
////                             ",variance(getcouponnum) getcouponnumVar " +
////                             "from circleDimTable group by circle_id")
////  circleDim.printSchema()
//
//  //LG模型训练
//
////  17/11/18 22:58:26 INFO DAGScheduler: Job 2 failed: reduce at VertexRDDImpl.scala:90, took 2945.480248 s
////  org.apache.spark.SparkException: Job aborted due to stage failure: ShuffleMapStage 7 (distinct at <console>:47) has failed the maximum allowable number of times: 4. Most recent failure reason: org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 0
//
//
//
//
//  //test
//  val testvalidConnect = connect_b.map(x=>(x._1,x._2,x._2.size))
//  testvalidConnect.saveAsTextFile("hdfs:///user/fuzheng/aa.txt")
//  //查看
//  testvalidConnect.map(x=>(x._1,x._3)).collect()
//
//
//
//
//
//  //统计信息
//    val s=testvalidConnect.map(x=>(x._3)).collect()
//    s.sum   //1093
//    s.length  //53
//    s.max
//  //抽样查看
//  val subGraph = connect_b.filter(x => x._1 ==100000005510922L).map(x=>x._2.toList).collect()
//  for(item <- subGraph.head) print((s",'${item}'"))
////  println("子图所有顶点：")
//////  subGraph.map(v => print((s",'${v._1}'")))
////  subGraph.vertices.collect.foreach(v => println(s"${v._1} is ${v._2}"))
////  println("子图所有边：")
////  subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//
//  //Degrees操作
//  println("找出图中最大的出度、入度、度数：")
//  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
//    if (a._2 > b._2) a else b
//  }
//  println("max of outDegrees:" + connect_a.outDegrees.reduce(max) + " max of inDegrees:" + connect_a.inDegrees.reduce(max) + " max of Degrees:" + connect_a.degrees.reduce(max))
//  println
//
//
//
//  //test
//// val circles_a = validConnect.vertices.filter(x=>x._2.toString=="100000002045602").collect.foreach(println(_))
//
//
////  val circle_test_a=circles_len.filter(x=>x._3>1)
////  val circle_test_b=circle_test_a.filter(x=> x._1.toString=="100000002045602").map(x=>x._1 :: x._2.toList).collect.foreach(println(_))
//
////  circle_test_a.first()
////  circle_test_a.count()
//
////
//
//
//
//  val subGraph = connect.subgraph(vpred = (id, vd) => vd ==100000006882640L)
//  println("子图所有顶点：")
//  ValidConnect.vertices.collect.foreach(v => print((s",'${v._1}'")))
//  ValidConnect.vertices.collect.foreach(v => println(s"${v._1} is ${v._2}"))
//  println("子图所有边：")
//  ValidConnect.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//
//  //Degrees操作
//  println("找出图中最大的出度、入度、度数：")
//  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
//    if (a._2 > b._2) a else b
//  }
//  println("max of outDegrees:" + connect.outDegrees.reduce(max) + " max of inDegrees:" + connect.inDegrees.reduce(max) + " max of Degrees:" + connect.degrees.reduce(max))
//  println
//
//
//
//
//  //
////  //双重循环，找出会员之间的关系值
////  val member_allInfo_a=member_allInfo.rdd.mapPartitions(rows =>{
////    rows.flatMap(Ylist=>{
////      list.map(Xlist =>  {
////        val memberPair = Set(Ylist(0),Xlist(0))
////        var memberDim: List[String] = List()
////        //最大：30
////        //+3 device_id,online_total_need_amt
////        //+2 IP,recept_phone,recept_name,recept_address_detail，orderdetailgoodscost，avg_detailorder_discount，avgcouponuse,getcouponsecdif
////        //+1 others
////        if (Xlist(1)!= "999999"  && Ylist(1)==Xlist(1) ) {memberDim=memberDim:+"IP"} //"IP"
////        if (Ylist(2)!= "999999"  && Ylist(2)==Xlist(2) ) {memberDim=memberDim:+"device_id"} //"device_id"
////        if (Ylist(3)!= "999999"  && Ylist(3)==Xlist(3) ) {memberDim=memberDim:+"recept_address_same"} //"recept_address_same"
////        if (Ylist(4)!= "999999"  && Ylist(4)==Xlist(4) ) {memberDim=memberDim:+"recept_address_detail"} //"recept_address_detail"
////        if (Ylist(5)!= "999999"  && Ylist(5)==Xlist(5) ) {memberDim=memberDim:+"recept_name"}//"recept_name"
////        if (Ylist(6)!= "999999"  && Ylist(6)==Xlist(6) ) {memberDim=memberDim:+"recept_phone"} //"recept_phone"
////        if (Ylist(7)!= "999999"  && Ylist(7)==Xlist(7) ) {memberDim=memberDim:+"register_phone"} //"register_phone"
//////        if (Ylist(8)!= "999999"  && Ylist(8)==Xlist(8) ) {memberDim=memberDim:+"recept_mobilearea_a"} //"recept_mobilearea_a"
//////        if (Ylist(9)!= "999999"  && Ylist(9)==Xlist(9) ) {memberDim=memberDim:+"register_mobilearea_a"} //"register_mobilearea_a"
////        if (Ylist(10)!= "999999" && getCoreTime(Ylist(10).asInstanceOf[String],Xlist(10).asInstanceOf[String]) ) {memberDim=memberDim:+"register_time"} //"register_time"
////        if (Ylist(11)!= "999999" && getCoreTime(Ylist(11).asInstanceOf[String],Xlist(11).asInstanceOf[String]) ) {memberDim=memberDim:+"online_first_sale_time"} //"online_first_sale_time"
////        if (Ylist(12)!= "999999" && getCoreTime(Ylist(12).asInstanceOf[String],Xlist(12).asInstanceOf[String]) ) {memberDim=memberDim:+"online_last_sale_time"} //"online_last_sale_time"
////        if (Ylist(13)!= 999999.0 && (Ylist(13).asInstanceOf[Float]-Xlist(13).asInstanceOf[Float]).abs<10 )      {memberDim=memberDim:+"online_first_need_amt"}  //"online_first_need_amt" 第一次支付差价在10元之内
////        if (Ylist(14)!= 999999.0 && (Ylist(14).asInstanceOf[Float]-Xlist(14).asInstanceOf[Float]).abs<10  )      {memberDim=memberDim:+"online_last_need_amt"} //"online_last_need_amt" 最后一次支付差价在10元之内
////        if (Ylist(15)!= 999999.0 && (Ylist(15).asInstanceOf[Float]-Xlist(15).asInstanceOf[Float]).abs<10  )      {memberDim=memberDim:+"online_total_need_amt"} //"online_total_need_amt" 总共支付差价在10元之内
////        if (Ylist(16)!= "999999" && getCoreTime(Ylist(16).asInstanceOf[String],Xlist(16).asInstanceOf[String]) ) {memberDim=memberDim:+"first_acquire_time"} //"first_acquire_time"
////        if (Ylist(17)!= "999999" && getCoreTime(Ylist(17).asInstanceOf[String],Xlist(17).asInstanceOf[String]) ) {memberDim=memberDim:+"last_acquire_time"} //"last_acquire_time"
////        if (Ylist(18)!= 999999.0 && (Ylist(18).asInstanceOf[Float] -Xlist(18).asInstanceOf[Float]).abs <10 && Xlist(18).asInstanceOf[Float]<=2000  ) {memberDim=memberDim:+"getcouponsecdif"} //"getcouponsecdif"
////        if (Ylist(19)!= 999999.0 && (Ylist(19).asInstanceOf[Float]-Xlist(19).asInstanceOf[Float]).abs < 10 )     {memberDim=memberDim:+"orderdetailgoodscost"}  //"orderdetailgoodscost"平均商品价格相差10元之内
////        if (Ylist(20)!= 999999.0 && (Ylist(20).asInstanceOf[Float]-Xlist(20).asInstanceOf[Float]).abs < 10 )     {memberDim=memberDim:+"avg_detailorder_discount" }   //"avg_detailorder_discount" 订单折扣均价相差10元之内
//////        if (Ylist(21)!= 999999.0 && (Ylist(21).asInstanceOf[Float]-Xlist(21).asInstanceOf[Float]).abs<= 2  )     {memberDim=memberDim:+"coupon_amount"}   //"coupon_amount" 领券数量相差2个及以下
////        if (Ylist(22)!= 999999.0 && (Ylist(22).asInstanceOf[Float]-Xlist(22).asInstanceOf[Float]).abs<=10  )     {memberDim=memberDim:+"avgcouponuse"} //"avgcouponuse" 每个商品平均折扣额 和 平均使用券额度之差，两个会员相差10元之内
//////        if (Ylist(23)!= 999999.0 && (Ylist(23).asInstanceOf[Float]-Xlist(23).asInstanceOf[Float]).abs<=2   )     {memberDim=memberDim:+"getcouponnum"}  //"getcouponnum"领取券的数量之差，在2个以内
////        (memberPair,memberDim)
////      })
////    })
////  }).distinct()
//
////  def splitEdgesAttr(memberPair:Set[Any],memberDim:List[String])={
////    for{dim<-memberDim} yield {List(memberPair.head,memberPair.last,dim.toString)}
////  }
////
////
////  val member_allInfo_b=member_allInfo_a.filter(x=>x._1.size>1).filter(x=>x._2.length>5)
////  val member_allInfo_c=member_allInfo_b.flatMap(x=>splitEdgesAttr(x._1,x._2))
////  val member_allInfo_Edge=member_allInfo_c.map(line =>(Edge(line(0).asInstanceOf[Long],line(1).asInstanceOf[Long],line(2).toString ))) //构建EdgeRDD
////  val member_allInfo_Ver =member_allInfo.rdd.map(x=>(x(0).asInstanceOf[Long],( (x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).asInstanceOf[Float],x(14).asInstanceOf[Float],x(15).asInstanceOf[Float],x(16).toString,x(17).toString,x(18).asInstanceOf[Float],x(19).asInstanceOf[Float],x(20).asInstanceOf[Float],x(21).asInstanceOf[Float]),(x(22).asInstanceOf[Float],x(23).asInstanceOf[Float]))))
////  val defaultUser = (111111111L, "Missing")
////  val graph: Graph[( (String, String, String, String, String, String, String, String, String, String, String, String, Float, Float, Float, String, String, Float, Float, Float, Float), (Float, Float)), String] = Graph(member_allInfo_Ver, member_allInfo_Edge)
////  val connect: Graph[VertexId, String]= graph.connectedComponents()
////
////
//
//
//
//  member_allInfo_Edge.first
//
//
//
//
//  val connect: Graph[VertexId, Int]= graph.connectedComponents()
//  val count = connect.vertices.collect()
//  val vert = connect.vertices.map( vertex => vertex._2).distinct().collect()//1,9
//  val graphs =ArrayBuffer[Graph[VertexId,Int]]()
//  for( attr <- vert ){
//    println(attr)
//    val subGraph = connect.subgraph( vpred = (id, value) =>  value == attr)
////    val  v= subGraph.vertices.distinct()
////    subGraph.vertices.collect.foreach(v => println(s"${v._1} is ${v._2}"))
//  }
//
//
//  println("**********************************************************")
//  println("结构操作")
//  println("**********************************************************")
//  println("顶点年纪>30的子图：")
//  val subGraph = connect.subgraph(vpred = (id, vd) => vd == 100000000001277L)
//  println("子图所有顶点：")
//  subGraph.vertices.collect.foreach(v => print(s",'${v._1}'"))
//  graph.vertices.collect.foreach(v => println(s"${v._1} is ${v._2._1}"))
//  println
//  println("子图所有边：")
//  subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//  println
//
//
//  val lp = lib.LabelPropagation.run(graph, 3).vertices
//  val LpByUsername = member_allInfo_Ver.join(lp).first
//
//    map {
//    case (id, (username, label)) =>
//      (username, label)
//  }
//
//
//
//  val ranks = graph.pageRank(0.0001).vertices
////  val ranksByUsername = member_allInfo_Ver.join(ranks).map {
////    case (id, (username, rank)) => (username, rank)
////  }
//  println(ranks.collect().mkString("\n"))
//  member_allInfo_Edge.filter(x=> x.equals("100000000000509")).collect()
//
//
//  val outputdir="D:/project/Scalper2/src/output"
//  var parallelism,minProgress,progressCounter = -1
//  val runner = new HDFSLouvainRunner(minProgress,progressCounter,outputdir)
//  runner.run(sc, graph)
//
////
////
////
////
////
////
//
////
////  member_allInfo_f.first()
//
//}
//
