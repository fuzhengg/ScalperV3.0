package com.bl

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.List


class memberCross  extends java.io.Serializable {
  def getproperty(key: String,keynum:String ,Score_ip: Double, sparkSql: SparkSession,num:Int):org.apache.spark.rdd.RDD[(scala.collection.immutable.Set[Long], Double)] = {
    //member_id,在同一条件下两两组合
    val key_member_valid =sparkSql.sql(s"select $key,bigint(member_id) member_id "+
                                       s"from HnMember_infoTable " +
                                       s"where $key!='999999' and $keynum>$num and $key is not null  and  $key<>''  and register_time!='999999'  and online_last_sale_time!='999999'  and online_first_sale_time!='999999' " +
                                       s"group by $key,bigint(member_id)  ")
    val key_member_valid_row = key_member_valid.rdd.map(x => x.toSeq.toList).map(x => (x(0).toString, List(x(1).toString.toLong)))
    val key_member_valid_combination_a = key_member_valid_row.reduceByKey((x, y) => (x ::: y))
    val key_member_valid_combination_b = key_member_valid_combination_a.repartition(1400).flatMapValues(x => x.combinations(2))
    val key_member_valid_combination_c = key_member_valid_combination_b.filter(x=>x._2(0)!=x._2(1))
    val key_member_valid_combination_d = key_member_valid_combination_c.map(x=>(Set(x._2(0),x._2(1)),Score_ip))
    key_member_valid_combination_d
  }

  def getZscore(sparkSql: SparkSession,HnMember_info:sql.DataFrame,ZscoreSchema:StructType): org.apache.spark.sql.DataFrame ={
    import sparkSql.implicits._
    //将特征对应的会员量进行标准化
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false)
    val HnMember_info_a=HnMember_info.select("member_id","device_id","recept_phone","recept_address_same","ip","recept_name"
      ,"ipphonenum","ipnamenum","ipaddressnum","devicenamenum","devicephonenum","deviceaddressnum","devmemnum","phonememnum","sameaddressmemnum","ipmemnum","namememnum")
    val HnMember_info_b=HnMember_info_a.rdd.map(line=> line.toSeq.map{_.toString}.toArray)
    val HnMember_info_c=HnMember_info_b.map(line=> (line(0),line(1),line(2),line(3),line(4),line(5),Vectors.dense(line.drop(6).map(_.toDouble))))
    val HnMember_info_d=HnMember_info_c.toDF("member_id","device_id","recept_phone","recept_address_same","ip","recept_name","features")
    val HnMember_info_dZscoreModel=scaler.fit(HnMember_info_d)
    val HnMember_info_dZscore=HnMember_info_dZscoreModel.transform(HnMember_info_d)
    //将ZSCORE提取出来
    //转化成RDD
    val HnMember_info_ZscoreA=HnMember_info_dZscore.rdd.map(line=>(line(0),line(1),line(2),line(3),line(4),line(5),line.getAs[DenseVector]("features"),line.getAs[DenseVector]("scaledFeatures")) )
    val HnMember_info_ZscoreB=HnMember_info_ZscoreA.map(line=>(line._1,line._2,line._3,line._4,line._5,line._6,line._7.toArray.map(_.toDouble),line._8.toArray.map(_.toDouble)))
    //转换DATAFrame 表
    val HnMember_info_ZscoreC=HnMember_info_ZscoreB.map(line=>Row(line._1.toString,line._2.toString,line._3.toString,line._4.toString,line._5.toString,line._6.toString,line._8(0),line._8(1),line._8(2),line._8(3),line._8(4),line._8(5),line._8(6),line._8(7),line._8(8),line._8(9),line._8(10)))
    val HnMember_info_ZscoreD=sparkSql.createDataFrame(HnMember_info_ZscoreC,ZscoreSchema)
    HnMember_info_ZscoreD.createOrReplaceTempView("HnMember_info_ZscoreD")
    //加入之前的特征
    HnMember_info.createOrReplaceTempView("HnMember_info_ini")
    val HnMember_info_Zscore=sparkSql.sql(s"select a.member_id,a.register_time,a.ip,a.device_id,a.recept_address_detail,a.recept_address_same,a.recept_name,a.recept_phone,a.ipphone,a.ipname,a.ipaddress,a.ipdevice,a.devicename,a.devicephone,a.deviceaddress,a.phoneaddress,a.phonename,a.nameaddress,a.ecp_ind,a.online_last_sale_time,a.online_last_sale_amt,a.online_last_discount_amt,a.online_last_need_amt,a.online_first_sale_time,a.online_first_sale_amt,a.online_first_discount_amt,a.online_first_need_amt,a.online_total_sale_amt,a.online_total_discount_amt,a.online_total_need_amt,a.couponacquire_num,a.couponlastacquire_time,a.couponlastacquire_type  " +
      s",a.ipphonenum,a.ipnamenum,a.ipaddressnum,a.devicenamenum,a.devicephonenum,a.deviceaddressnum,a.devmemnum,a.phonememnum,a.sameaddressmemnum,a.ipmemnum,a.namememnum    " +
      s",b.ipphonenumZScore,b.ipnamenumZScore,b.ipaddressnumZScore,   b.devicenamenumZScore,b.devicephonenumZScore  ,b.deviceaddressnumZScore ,b.devmemnumZScore,b.phonememnumZScore,b.sameaddressmemnumZScore,b.namememnumZScore,b.ipmemnumZScore  " +
      s"from  HnMember_info_ZscoreD b join HnMember_info_ini a on a.member_id=b.member_id and a.ip=b.ip and a.device_id=b.device_id and a.recept_address_same=b.recept_address_same and a.recept_name=b.recept_name and a.recept_phone=b.recept_phone  " +
      s"group by a.member_id,a.register_time,a.ip,a.device_id,a.recept_address_detail,a.recept_address_same,a.recept_name,a.recept_phone,a.ipphone,a.ipname,a.ipaddress,a.ipdevice,a.devicename,a.devicephone,a.deviceaddress,a.phoneaddress,a.phonename,a.nameaddress,a.ecp_ind,a.online_last_sale_time,a.online_last_sale_amt,a.online_last_discount_amt,a.online_last_need_amt,a.online_first_sale_time,a.online_first_sale_amt,a.online_first_discount_amt,a.online_first_need_amt,a.online_total_sale_amt,a.online_total_discount_amt,a.online_total_need_amt,a.couponacquire_num,a.couponlastacquire_time,a.couponlastacquire_type  " +
      s",a.ipphonenum,a.ipnamenum,a.ipaddressnum,a.devicenamenum,a.devicephonenum,a.deviceaddressnum,a.devmemnum,a.phonememnum,a.sameaddressmemnum,a.ipmemnum,a.namememnum    " +
      s",b.ipphonenumZScore,b.ipnamenumZScore,b.ipaddressnumZScore,   b.devicenamenumZScore,b.devicephonenumZScore  ,b.deviceaddressnumZScore ,b.devmemnumZScore,b.phonememnumZScore,b.sameaddressmemnumZScore,b.namememnumZScore,b.ipmemnumZScore   ")
    HnMember_info_Zscore
  }


  def getMemberCross( sparkSql: SparkSession,HnMember_info:sql.DataFrame):org.apache.spark.rdd.RDD[(scala.collection.immutable.Set[Long], Double)] = {
    //假设：device>phone>add>ip>name，且下单人和收货人组合中，device组合>IP组合
    //令： device=0.4; phone=0.3;  add=0.15>;  ip=0.1;  name=0.05     同时，device组合+0.2； IP组合+0.1
    //得：最小组合0.15，最大组合0.9；
    val Score_device_id = 0.4
    val Score_recept_phone =0.3
    val Score_ip = 0.2
    val Score_recept_address_same = 0.15
    val Score_recept_name = 0.05

    //device组合+0.2
    val Score_deviceName = 0.2
    val Score_devicePhone = 0.2
    val Score_deviceAddress = 0.2

    //IP组合+0.1
    val Score_ipPhone = 0.15
    val Score_ipName = 0.15
    val Score_ipAddress = 0.15

    val ZscoreSchema = StructType(
      Seq(
        StructField("member_id", StringType  , true )
        ,StructField("device_id", StringType  , true )
        ,StructField("recept_phone", StringType  , true )
        ,StructField("recept_address_same", StringType  , true )
        ,StructField("ip", StringType  , true )
        ,StructField("recept_name", StringType  , true )
        ,StructField("ipphonenumZScore",DataTypes.DoubleType , true )
        ,StructField("ipnamenumZScore",DataTypes.DoubleType , true)
        ,StructField("ipaddressnumZScore",DataTypes.DoubleType , true )
        ,StructField("devicenamenumZScore",DataTypes.DoubleType , true )
        ,StructField("devicephonenumZScore", DataTypes.DoubleType , true )
        ,StructField("deviceaddressnumZScore", DataTypes.DoubleType , true)
        ,StructField("devmemnumZScore",DataTypes.DoubleType , true )
        ,StructField("phonememnumZScore",DataTypes.DoubleType , true)
        ,StructField("sameaddressmemnumZScore",DataTypes.DoubleType , true )
        ,StructField("ipmemnumZScore",DataTypes.DoubleType , true )
        ,StructField("namememnumZScore",DataTypes.DoubleType , true )
      )
    )



    //将特征对应的会员量进行标准化
    val HnMember_info_numZscore=getZscore(sparkSql,HnMember_info,ZscoreSchema).repartition(1400).persist(StorageLevel.MEMORY_AND_DISK_2)
    HnMember_info_numZscore.createOrReplaceTempView("HnMember_infoTable")

//    val dd=sparkSql.sql("select * from algorithm_data.HnMember_info WHERE device_id IN ('9be26153a43e43c486373cfac0e10263')").collect()
//    val da=sparkSql.sql("select * from HnMember_infoTable WHERE device_id IN ('9be26153a43e43c486373cfac0e10263')").collect()
//    val da=sparkSql.sql("select * from HnMember_infoTable WHERE member_id IN ('9be26153a43e43c486373cfac0e10263')").collect()
//    da.filter(x=>x(0)=="100000008814317")
//    for (elem <- da) println(elem)
    //会员根据固定维度判断是否违规
    val hn_device_id = getproperty("device_id","devmemnum", Score_device_id,sparkSql,2)   //1816374
    val hn_recept_phone = getproperty("recept_phone","phonememnum", Score_recept_phone, sparkSql,2) //66306
    val hn_recept_address_same = getproperty("recept_address_same","sameaddressmemnumZScore", Score_recept_address_same, sparkSql,1) //25233904
    val hn_ip = getproperty("ip","ipmemnum", Score_ip, sparkSql,2)  // 1385510
    val hn_recept_name = getproperty("recept_name", "namememnumZScore",Score_recept_name, sparkSql,1) //265358

    val hn_deviceName = getproperty("deviceName", "deviceNameNum",Score_deviceName, sparkSql,2)
    val hn_devicePhone = getproperty("devicePhone","devicePhoneNum", Score_devicePhone, sparkSql,2)
    val hn_deviceAddress = getproperty("deviceAddress","deviceAddressNum", Score_deviceAddress, sparkSql,2)

    val hn_ipPhone = getproperty("ipPhone","ipPhoneNum", Score_ipPhone, sparkSql,2)
    val hn_ipName = getproperty("ipName","ipNameNum", Score_ipName,sparkSql,2)
    val hn_ipAddress = getproperty("ipAddress","ipAddressNum", Score_ipAddress, sparkSql,2)

    //固定维度混合，然后判断是否违规
    //1.两两合并

    val ip_deviceId = hn_ip.fullOuterJoin(hn_device_id).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val name_receptPhone = hn_recept_name.fullOuterJoin(hn_recept_phone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val hn_phoneAddress = hn_deviceAddress.fullOuterJoin(hn_recept_address_same).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))


//    val hn_phoneAddress = hn_deviceAddress.fullOuterJoin(hn_recept_address_same).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] +x._2._2.getOrElse(0.0).asInstanceOf[Double]))
//    hn_device_id.filter(x=>x._1.contains("100000008809931".toString.toLong)).collect()


    val deviceNamePhone = hn_deviceName.fullOuterJoin(hn_devicePhone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val ipPhoneName = hn_ipPhone.fullOuterJoin(hn_ipName).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    //2.
    val ipPhoneipNameipAddress = ipPhoneName.fullOuterJoin(hn_ipAddress).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val ipDeviceNamePhone=ip_deviceId.fullOuterJoin(name_receptPhone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val addressDevAddressDevNameDevPhone=hn_phoneAddress.fullOuterJoin(deviceNamePhone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    //3.最终得分
    val addressIPDEV=ipPhoneipNameipAddress.fullOuterJoin(addressDevAddressDevNameDevPhone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double]))
    val all_dim = addressIPDEV.fullOuterJoin(ipDeviceNamePhone).map(x => (x._1, x._2._1.getOrElse(0.0).asInstanceOf[Double] + x._2._2.getOrElse(0.0).asInstanceOf[Double])).persist(StorageLevel.MEMORY_AND_DISK_2)

//    val aa=all_dim.map(x=>(x._2,x._1)).countByKey()
//    for (elem <- aa) println(elem)
    //(Set(100000008561153, 100000000013560),0.15)
//    val ee = hn_phoneAddress.filter(x=>x._1.contains("100000008809928".toString.toLong)).collect()
    return all_dim
  }
}

