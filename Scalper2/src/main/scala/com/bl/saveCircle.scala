package com.bl


import org.apache.spark.sql.SparkSession

class saveCircle  extends java.io.Serializable {
//  case class connectFiltedSchema(circle_id:Long,circle_size:Long,circle_members:Array[Long])
  def saveCircleTable(Year:Int,Month:Int,Day:Int,sparkSql:SparkSession,connectFilted:org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, Iterable[org.apache.spark.graphx.VertexId])],connectFiltedTableDf:org.apache.spark.sql.DataFrame)= {
    //1. transfer the result to Table
    connectFiltedTableDf.createOrReplaceTempView("connectFiltedTable")
    //2. add some dims and to new Table
    sparkSql.sql("set hive.cli.print.header=true") // 打印列名
    sparkSql.sql("set hive.cli.print.row.to.vertical=true") // 开启行转列功能, 前提必须开启打印列名功能
    sparkSql.sql(s"insert overwrite table algorithm_data.hnallorder_circleResult PARTITION (year=$Year,month=$Month,day=$Day) " +
      s"select   aa.circle_id,aa.circle_size,aa.circle_memberID,b.ip,b.device_id,b.recept_address_same,b.recept_address_detail" +
      s",b.recept_name,b.recept_phone,b.register_phone,b.recept_mobilearea_a,b.register_mobilearea_a,b.register_time,b.online_first_sale_time" +
      s",b.online_last_sale_time,b.online_first_need_amt,b.online_last_need_amt,b.online_total_need_amt,b.first_acquire_time,b.last_acquire_time" +
      s",b.getcouponsecdif,b.orderdetailgoodscost,b.avg_detailorder_discount,b.coupon_amount,b.avgcouponuse,b.getcouponnum " +
      s"from (SELECT circle_id,circle_size,circle_memberID FROM  connectFiltedTable  a   lateral view explode(a.circle_members) adtable as circle_memberID) aa " +
      s"join algorithm_data.HnAllOrder_spark b on aa.circle_memberID=b.member_id ")
  }
}
