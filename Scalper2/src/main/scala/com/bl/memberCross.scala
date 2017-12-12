package com.bl

import org.apache.spark.sql.SparkSession

class memberCross  extends java.io.Serializable  {
    def getMemberCross(Year:Int,Month:Int,sparkSql:SparkSession)= {
        //会员笛卡尔积
        val hnallorder_spark_cross = sparkSql.sql(s"SELECT * FROM  algorithm_data.allconsummember WHERE year=$Year AND  month=$Month ").coalesce(300)
//        val hnallorder_spark_cross = sparkSql.sql(s"SELECT * FROM  algorithm_data.allconsummember WHERE year=$Year ").coalesce(300)
        hnallorder_spark_cross.createOrReplaceTempView("hnallorder_spark_crossTable")

        val crossMemberInfo = sparkSql.sql("select b.member_id  Xmember_id,b.ip Xip,b.device_id Xdevice_id" +
          ",b.recept_address_same Xrecept_address_same,b.recept_address_detail Xrecept_address_detail" +
          ",b.recept_name Xrecept_name,b.recept_phone Xrecept_phone,b.register_phone Xregister_phone" +
          ",b.recept_mobilearea_a Xrecept_mobilearea_a,b.register_mobilearea_a Xregister_mobilearea_a" +
          ",b.register_time Xregister_time,b.online_first_sale_time Xonline_first_sale_time" +
          ",b.online_last_sale_time Xonline_last_sale_time,b.online_first_need_amt Xonline_first_need_amt" +
          ",b.online_last_need_amt Xonline_last_need_amt,b.online_total_need_amt Xonline_total_need_amt" +
          ",b.first_acquire_time Xfirst_acquire_time,b.last_acquire_time Xlast_acquire_time,b.getcouponsecdif Xgetcouponsecdif" +
          ",b.orderdetailgoodscost Xorderdetailgoodscost,b.avg_detailorder_discount Xavg_detailorder_discount,b.coupon_amount Xcoupon_amount" +
          ",b.avgcouponuse Xavgcouponuse,b.getcouponnum Xgetcouponnum,c.member_id  Ymember_id,c.ip Yip,c.device_id Ydevice_id" +
          ",c.recept_address_same Yrecept_address_same,c.recept_address_detail Yrecept_address_detail,c.recept_name Yrecept_name" +
          ",c.recept_phone Yrecept_phone,c.register_phone Yregister_phone,c.recept_mobilearea_a Yrecept_mobilearea_a" +
          ",c.register_mobilearea_a Yregister_mobilearea_a,c.register_time Yregister_time,c.online_first_sale_time Yonline_first_sale_time" +
          ",c.online_last_sale_time Yonline_last_sale_time,c.online_first_need_amt Yonline_first_need_amt" +
          ",c.online_last_need_amt Yonline_last_need_amt,c.online_total_need_amt Yonline_total_need_amt,c.first_acquire_time Yfirst_acquire_time" +
          ",c.last_acquire_time Ylast_acquire_time,c.getcouponsecdif Ygetcouponsecdif,c.orderdetailgoodscost Yorderdetailgoodscost" +
          ",c.avg_detailorder_discount Yavg_detailorder_discount,c.coupon_amount Ycoupon_amount,c.avgcouponuse Yavgcouponuse" +
          ",c.getcouponnum Ygetcouponnum  " +
          "from hnallorder_spark_crossTable a  " +
          "join algorithm_data.hnallorder_spark b on a.Xmember_id=b.member_id  " +
          "join algorithm_data.hnallorder_spark c on a.Ymember_id=c.member_id  ")
        crossMemberInfo.createOrReplaceTempView("crossMemberInfoTable")
        //    crossMemberInfo.printSchema()

        //4. member relations
        sparkSql.sql("drop table if exists algorithm_data.hnallorder_spark_crossInfo")
        sparkSql.sql("create table algorithm_data.hnallorder_spark_crossInfo AS  " +
          "select Xmember_id,Ymember_id " +
          ",case when string(Xip) <>'999999' and Xip=Yip  then 0.5    else  0  end   as ip  " +
          ",case when string(Xdevice_id) <>'999999' and Xdevice_id=Ydevice_id  then 2.0    else   0  end   as device_id  " +
          ",case when string(Xrecept_address_same) <>'999999' and Xrecept_address_same=Yrecept_address_same  then 0.5    else 0    end   as recept_address_same  " +
          ",case when string(Xrecept_address_detail) <>'999999' and Xrecept_address_detail=Yrecept_address_detail  then  1.0   else   0  end   as recept_address_detail  " +
          ",case when string(Xrecept_name) <>'999999' and Xrecept_name=Yrecept_name  then  1.0   else  0   end   as  recept_name " +
          ",case when string(Xrecept_phone) <>'999999' and Xrecept_phone=Yrecept_phone  then  1.0   else  0  end   as recept_phone  " +
          ",case when string(Xregister_phone) <>'999999' and Xregister_phone=Yregister_phone  then   1.0  else  0  end   as  register_phone " +
          ",case when string(Xrecept_mobilearea_a) <>'999999' and Xrecept_mobilearea_a= Yrecept_mobilearea_a then  0   else 0    end   as  recept_mobilearea_a " +
          ",case when string(Xregister_mobilearea_a) <>'999999' and Xregister_mobilearea_a=Yregister_mobilearea_a  then 0    else  0   end   as  register_mobilearea_a " +
          ",case when string(Xregister_time) <>'999999' and abs(unix_timestamp(Xregister_time)-unix_timestamp(Yregister_time))<600  then  0.2  else    0 end   as  register_time " +
          ",case when string(Xonline_first_sale_time) <>'999999' and abs(unix_timestamp(Xonline_first_sale_time)-unix_timestamp(Yonline_first_sale_time))<600  then  0.1   else 0    end   as  online_first_sale_time " +
          ",case when string(Xonline_last_sale_time) <>'999999' and abs(unix_timestamp(Xonline_last_sale_time)-unix_timestamp(Yonline_last_sale_time))<600  then   0.1  else   0  end   as  online_last_sale_time " +
          ",case when string(Xonline_first_need_amt) <>'999999' and abs(Xonline_first_need_amt-Yonline_first_need_amt)<10  then  0   else  0   end   as  online_first_need_amt " +
          ",case when string(Xonline_last_need_amt) <>'999999' and abs(Xonline_last_need_amt-Yonline_last_need_amt)<10  then  0   else 0    end   as  online_last_need_amt " +
          ",case when string(Xonline_total_need_amt) <>'999999' and abs(Xonline_total_need_amt-Yonline_total_need_amt)<10  then  0.3   else  0   end   as  online_total_need_amt " +
          ",case when string(Xfirst_acquire_time) <>'999999' and abs(unix_timestamp(Xfirst_acquire_time)-unix_timestamp(Yfirst_acquire_time))<600  then 0.1    else  0   end   as  first_acquire_time " +
          ",case when string(Xlast_acquire_time) <>'999999' and abs(unix_timestamp(Xlast_acquire_time)-unix_timestamp(Ylast_acquire_time))<600  then   0.1  else  0   end   as  last_acquire_time " +
          ",case when string(Xgetcouponsecdif) <>'999999' and Xgetcouponsecdif<=20 and  abs(Xgetcouponsecdif-Ygetcouponsecdif) < 10   then  0.3   else 0     end   as  getcouponsecdif " + //领取优惠券和发放优惠券时间间隔在20min内，且两个会员领取间隔在10MIN内
          ",case when string(Xorderdetailgoodscost) <>'999999' and abs(Xorderdetailgoodscost-Yorderdetailgoodscost) < 10   then 0.2    else  0  end   as  orderdetailgoodscost " +
          ",case when string(Xavg_detailorder_discount) <>'999999' and abs(Xavg_detailorder_discount-Yavg_detailorder_discount) < 10   then 0.2     else 0     end   as  avg_detailorder_discount " + //价格相差10元之内
          ",case when string(Xcoupon_amount) <>'999999' and abs(Xcoupon_amount-Ycoupon_amount) < 2.0   then  0   else  0    end   as  coupon_amount " + //相差2元及以下
          ",case when string(Xavgcouponuse) <>'999999' and abs(Xavgcouponuse-Yavgcouponuse) < 10   then  0.1   else  0    end   as  avgcouponuse  " + //价格相差10元之内
          ",case when string(Xgetcouponnum) <>'999999' and abs(Xgetcouponnum- Ygetcouponnum) < 2.0  then  0   else  0    end   as  getcouponnum  " + //领取券的数量之差，在2个以内
          "from crossMemberInfoTable")
    }
}