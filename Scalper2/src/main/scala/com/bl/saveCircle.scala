package com.bl


import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel


class saveCircle  extends java.io.Serializable {
  def circleScore(Year:Int,Month:Int,Day:Int,cg:String,sparkSql:SparkSession,connectFiltedTableDf:org.apache.spark.sql.DataFrame,HnMember_info:org.apache.spark.sql.DataFrame)= {
    //一，根据注册和消费信息打分
    //1. transfer the result to Table
    connectFiltedTableDf.createOrReplaceTempView("connectFiltedTable")
    HnMember_info.createOrReplaceTempView("HnMember_info")
    val HnCircleResult_a = sparkSql.sql(s"select aa.circle_id,aa.circle_size,aa.circle_memberID " +
      s",b.register_time, b.ip,b.device_id,b.recept_address_same ,b.recept_address_detail ,b.recept_name,b.recept_phone " +
      s"from connectFiltedTable aa " +
      s"join HnMember_info b on aa.circle_memberID=b.member_id ")
    HnCircleResult_a.createOrReplaceTempView("test_HnStatics")
//    Statics_ini_data_a.printSchema()

    //1.1  get iniTable for Score
    val Statics_ini_data_a =  sparkSql.sql(s"select distinct  c.circle_id,c.circle_size,c.circle_memberid,c.register_time " +
      s",c.device_id,c.ip,c.recept_address_same,c.recept_address_detail,c.recept_name,c.recept_phone    ,a.order_no,a.order_valid_ind  " +
      s",case when from_unixtime(unix_timestamp(a.sale_time),'yyyy-MM-dd HH:mm:ss')  is null then  '999999' else from_unixtime(unix_timestamp(a.sale_time),'yyyy-MM-dd HH:mm:ss')   end as  sale_time " +
      s",a.need_money as order_need_money,a.ticket_money as order_ticket_money,a. discount_money_sum as Orderdiscount_money_sum " +
      s"from  test_HnStatics c " +
      s"join algorithm_data.HnOrder_info a on bigint(c.circle_memberid)=bigint(a.member_id) and a.recept_phone=c.recept_phone and a.recept_name=c.recept_name and a.recept_address_detail=c.recept_address_detail    " )
    Statics_ini_data_a.createOrReplaceTempView("HnAllOrder_e")
    sparkSql.sql("CACHE TABLE HnAllOrder_e")



    //1.2注册时间评分
    val Statics_register_a = sparkSql.sql(s"select circle_id,circle_size,register_date,register_hour,circle_membernum  "+
      s",ROW_NUMBER() OVER(PARTITION BY circle_id,circle_size,register_date ORDER BY circle_membernum desc) AS rn   "+
      s"from (select  circle_id,circle_size  "+
      s"      ,to_date(register_time) register_date,hour(register_time) register_hour  "+
      s"      ,count(distinct circle_memberid) circle_membernum  "+
      s"      from HnAllOrder_e  "+
      s"      where register_time is not null   "+
      s"      group by circle_id,circle_size,to_date(register_time),hour(register_time)  "+
      s"      )a  "+
      s"where circle_membernum>2  ")
    Statics_register_a.createOrReplaceTempView("registerScore_aTable")
    val Statics_register_ab =sparkSql.sql(s"select a.circle_id,a.circle_size,a.register_date,a.register_hour,a.circle_membernum  "+
      s",stddev_pop(3600*hour(register_time)+60*minute(register_time)+second(register_time)) std_register_time  "+
      s"from (select * from registerScore_aTable where rn=1) a  "+
      s"join HnAllOrder_e b on a.circle_id=b.circle_id and a.circle_size=b.circle_size and a.register_date=to_date(b.register_time) and a.register_hour=hour(register_time)  "+
      s"group by  a.circle_id,a.circle_size,a.register_date,a.register_hour,a.circle_membernum  ")
    Statics_register_ab.createOrReplaceTempView("Statics_register_abTable")
    val Statics_register_b = sparkSql.sql(s"select a.circle_id,a.circle_size  "+
      s",count(distinct b.circle_memberid) circleSta_membernum,avg(std_register_time) std_register_time  "+
      s",1/(  (a.circle_size/count(distinct b.circle_memberid))* case when avg(std_register_time)=0.0 then 0.001 else avg(std_register_time) end)   registerTime_score  "+
      s"from Statics_register_abTable a   "+
      s"join HnAllOrder_e b on a.circle_id=b.circle_id and a.circle_size=b.circle_size and a.register_date=to_date(b.register_time) and a.register_hour=hour(register_time)  "+
      s"group by a.circle_id,a.circle_size  ")
    Statics_register_b.createOrReplaceTempView("Statics_register_bTable")

    //1.3 device_id
    val Statics_deviceID_a=sparkSql.sql(s"select  circle_id,circle_size  "+
      s",pow(count(distinct circle_memberid),2)/(circle_size*circle_size*count(distinct device_id)) deviceidScore  "+
      s",count(distinct circle_memberid) circle_membernum  "+
      s",count(distinct device_id) device_num  "+
      s"from HnAllOrder_e  "+
      s"where register_time is not null  and  device_id not in ('999999','') and device_id is not null  "+
      s"group by circle_id,circle_size  ")
    Statics_deviceID_a.createOrReplaceTempView("Statics_deviceID_aTable")

    //1.4 消费时间金额
    val Statics_consum_a=sparkSql.sql(s"select circle_id,circle_size,sale_date,sale_hour,circle_ordernum,circle_orderHourMembernum "+
      s",ROW_NUMBER() OVER(PARTITION BY circle_id,circle_size,sale_date ORDER BY circle_orderHourMembernum desc) AS rn  "+
      s"from (select  circle_id,circle_size "+
      s"      ,to_date(sale_time) sale_date,hour(sale_time) sale_hour "+
      s"      ,count(distinct order_no) circle_ordernum "+
      s"      ,count(distinct circle_memberid) circle_orderHourMembernum "+
      s"      from HnAllOrder_e   "+
      s"      where int(order_valid_ind)=1 "+
      s"      group by circle_id,circle_size,to_date(sale_time),hour(sale_time) "+
      s"      )a "+
      s"where circle_orderHourMembernum>2 ")
    Statics_consum_a.createOrReplaceTempView("consumScore_aTable")


    val Statics_consum_b=sparkSql.sql(s"select a.circle_id,a.circle_size,a.sale_date,a.sale_hour,a.circle_ordernum,a.circle_orderHourMembernum "+
      s",stddev_pop(3600*hour(b.sale_time)+60*minute(b.sale_time)+second(b.sale_time)) std_sale_time "+
      s",stddev_pop(order_need_money) std_needMoney "+
      s",stddev_pop(Orderdiscount_money_sum) std_discountMoney "+
      s"from (select a.*  "+
      s"      from consumScore_aTable a  "+
      s"      where rn=1) a "+
      s"join HnAllOrder_e b on  a.circle_id=b.circle_id and a.circle_size=b.circle_size   and     to_date(b.sale_time)=a.sale_date  and  hour(b.sale_time)=a.sale_hour  "+
      s"group by   a.circle_id,a.circle_size,a.sale_date,a.sale_hour,a.circle_ordernum,a.circle_orderHourMembernum ")
    Statics_consum_b.createOrReplaceTempView("Statics_consum_bTable")


    val Statics_consum_c=sparkSql.sql(s"select a.circle_id,a.circle_size  "+
      s",avg(std_sale_time) std_sale_time,avg(std_needMoney) std_needMoney,avg(std_discountMoney) std_discountMoney  "+
      s",1/((a.circle_size/count(distinct c.circle_memberid))*case when floor(avg(std_sale_time))=0 then 0.001 else avg(std_sale_time) end )  saleTime_Score  "+
      s",1/((a.circle_size/count(distinct c.circle_memberid))*case when floor(avg(std_needMoney))=0 then 0.001 else avg(std_needMoney)  end )  needMoney_Score  "+
      s",1/((a.circle_size/count(distinct c.circle_memberid))*case when floor(avg(std_discountMoney))=0 then 0.001 else  avg(std_discountMoney) end )  discountMoney_Score  "+
      s"from Statics_consum_bTable a   "+
      s"join HnAllOrder_e c on a.circle_id=c.circle_id and a.circle_size=c.circle_size   and to_date(c.sale_time)=a.sale_date  and  hour(c.sale_time)=a.sale_hour "+
      s"group by a.circle_id,a.circle_size  ")
    Statics_consum_c.createOrReplaceTempView("Statics_consum_cTable")



    //1.5 注册时间、device_id、消费时间金额汇总
    val Statics_all_a=sparkSql.sql("select  a.circle_id,a.circle_size  " +
      ",a.registerTime_score,b.deviceidScore,c.saleTime_Score,c.needMoney_Score,c.discountMoney_Score " +
      "from Statics_register_bTable a " +
      "join Statics_deviceID_aTable b on a.circle_id=b.circle_id and a.circle_size=b.circle_size   " +
      "join Statics_consum_cTable c on  a.circle_id=c.circle_id and a.circle_size=c.circle_size   ")
//    Statics_all_a.createOrReplaceTempView("socialScore_consumTable")



    //2 归一化
    import sparkSql.implicits._

    val scal = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val Statics_all_aa=Statics_all_a.rdd.map(line=> line.toSeq.map{_.toString}.toArray).persist(StorageLevel.MEMORY_AND_DISK_2)
    val Statics_all_ab=Statics_all_aa.map{line=> (line(0),line(1),Vectors.dense(line.drop(2).map(_.toDouble)))}
    val Statics_all_ac= Statics_all_ab.toDF("circle_id","circle_size", "features")
    val StaticsData_model = scal.fit(Statics_all_ac)
    val Statics_all_ad = StaticsData_model.transform(Statics_all_ac)

    //2.1 经归一化后的信息转化为RDD
    val Statics_all_ae =  Statics_all_ad.rdd.map(line=>(line(0),line(1),line.getAs[DenseVector]("features"),line.getAs[DenseVector]("scaledFeatures")) )
    val Statics_all_af =  Statics_all_ae.map(line=>(line._1,line._2,line._4.toArray.map(_.toDouble)))
    val Statics_all_ag =  Statics_all_af.map(line=>Row(line._1.toString,line._2.toString,line._3(0),line._3(1),line._3(2),line._3(3),line._3(4)))
    val Statics_all_ah =  Statics_all_ag.map(line=>Row(line(0).toString,line(1).toString   ,line(2),line(3),line(4),line(5),line(6) ,0.2*line(2).asInstanceOf[Double]+0.2*line(3).asInstanceOf[Double]+0.2*line(4).asInstanceOf[Double]+0.2*line(5).asInstanceOf[Double]+0.2*line(6).asInstanceOf[Double]   ))

    //2.2 整合后的分数归一化
    val scal_score = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val Statics_all_ai=Statics_all_ah.map(line=> line.toSeq.map{_.toString}.toArray).persist(StorageLevel.MEMORY_AND_DISK_2)
    val Statics_all_aj=Statics_all_ai.map{line=> (line(0),line(1),line(2),line(3),line(4),line(5),line(6) ,Vectors.dense(line.drop(7).map(_.toDouble)))}
    val Statics_all_ak= Statics_all_aj.toDF("circle_id","circle_size","registerTime_score","deviceidScore","saleTime_Score", "needMoney_Score","discountMoney_Score","features")
    val StaticsScore_model = scal_score.fit(Statics_all_ak)
    val Statics_all_al = StaticsScore_model.transform(Statics_all_ak)

    //2.3 转化成RDD
    val Statics_all_am =  Statics_all_al.rdd.map(line=>(line(0),line(1),line(2),line(3),line(4),line(5),line(6) ,line.getAs[DenseVector]("features"),line.getAs[DenseVector]("scaledFeatures")) )
    val Statics_all_an =  Statics_all_am.map(line=>(line._1,line._2,line._3,line._4,line._5,line._6,line._7,line._8.toArray.map(_.toDouble),line._9.toArray.map(_.toDouble)))
    val Statics_all_ao =  Statics_all_an.map(line=>Row(line._1.toString,line._2.toString,line._3,line._4,line._5,line._6,line._7,line._8(0),line._9(0)))
    val Statics_all_ap =  Statics_all_ao.map(line=>Row(line(0).toString,line(1).toString   ,line(2).toString,line(3).toString,line(4).toString,line(5).toString,line(6).toString,line(8).toString ))

    //2.2 结果保存为DataFrame
    val StaticsAllStruct = StructType(
      Seq(
        StructField("circle_id",StringType,true)
        ,StructField("circle_size",StringType,true)
        ,StructField("registerTime_score",StringType,true)
        ,StructField("deviceidScore",StringType,true)
        ,StructField("saleTime_Score",StringType,true)
        ,StructField("needMoney_Score",StringType,true)
        ,StructField("discountMoney_Score",StringType,true)
        ,StructField("Score",StringType,true)
      )
    )
    val StaticsAllDf=sparkSql.createDataFrame(Statics_all_ap,StaticsAllStruct)
    StaticsAllDf.createOrReplaceTempView("StaticsAllDfTable")
//    StaticsAllDf.printSchema()

    //3，结果保存
    sparkSql.sql(s"insert overwrite table algorithm_data.hnCircleScore PARTITION (year=2018,month=4,day=17,category='20180322nonRealTime') " +
//    sparkSql.sql(s"insert overwrite table algorithm_data.hnCircleScore PARTITION (year=$Year,month=$Month,day=$Day,category='$cg') " +
      s"select  b.circle_id,b.circle_size,a.Score,b.circle_memberid,b.register_time" +
      s",b.device_id,b.ip,b.recept_name,count(distinct b.order_no) order_num  " +
      s",a.registerTime_score,a.deviceidScore,a.saleTime_Score,a.needMoney_Score,a.discountMoney_Score  " +
      s"from HnAllOrder_e  b " +
      s"left join StaticsAllDfTable a on a.circle_id=b.circle_id and a.circle_size =b.circle_size  " +
      s"group by b.circle_id,b.circle_size,a.Score,b.circle_memberid,b.register_time,b.device_id,b.ip,b.recept_name  " +
      s",a.registerTime_score,a.deviceidScore,a.saleTime_Score,a.needMoney_Score,a.discountMoney_Score  ")
    sparkSql.sql("UNCACHE TABLE HnAllOrder_e")
  }
}
