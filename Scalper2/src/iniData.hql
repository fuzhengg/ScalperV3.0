
-----------------------------取有销售数据的 社交圈数据
-------------------------一, 会员基本信息
--------------first  订单,会员,商品详细信息
use algorithm_data;
drop table if exists HnAllOrder_aa;
create table HnAllOrder_aa as
select a.*
,b.order_detail_no,b.goods_name,b.goods_code
,b.sale_sum as goods_saleSum
,b.item_amount goods_item_amount,b.sale_price goods_sale_price
,b.sale_price*b.sale_sum-b.item_amount as detailOrder_discount
FROM  mdata.c_order_snap a
join  mdata.c_order_detail b on a.order_no=b.order_no
WHERE a.order_va_ind='1' and a.online_order_ind='1' and int(b.sale_sum)>0  and float(a.discount_money_sum)>3
and to_date(a.sale_time)>='2016-05-19'
;

use algorithm_data;
drop table if exists HnAllOrder_ab;
create table HnAllOrder_ab as
select a.*,c.recept_phone
,e.mobile AS register_Phone,e.email,e.cust_type,e.nickname,e.member_name,e.gender,e.birth_type,e.age,e.birth_year
,e.birth_month,e.birth_day,e.marriage_status,e.cert_type,e.cert_code,e.address,e.tel,e.edu_level,e.hobby
,e.industry,e.title,e.province,e.city,e.district_sub_info,e.district_desc,e.district,e.family_member_num,e.children_num
,e.income_level,e.black_flag,e.first_belong_bu_id,e.first_belong_store_id,e.member_level_id,e.register_time,e.create_time,e.ECP_ind
,e.member_status,e.register_store_id,e.register_bu_id,e.channel_id,e.login_code,e.ad_channel,e.ad_id,e.ay_bind_time,e.a_points
,e.a_remain_points,e.a_used_points,e.a_delay_points,e.b1_points,e.b1_remain_points,e.b1_used_points,e.b1_delay_points
,e.offline_last_bill_id,e.offline_last_store_id,e.offline_last_bu_id,e.offline_last_sale_time,e.offline_last_sale_amt
,e.offline_last_sale_cnt,e.offline_total_sale_amt,e.offline_total_sale_cnt,e.offline_first_bill_id,e.offline_first_store_id
,e.offline_first_bu_id,e.offline_first_sale_time,e.offline_first_sale_amt,e.offline_first_sale_cnt,e.online_last_order_no
,e.online_last_parent_order_no,e.online_last_sale_time,e.online_last_sale_amt,e.online_last_discount_amt,e.online_last_need_amt
,e.online_total_sale_amt,e.online_total_discount_amt,e.online_total_need_amt,e.online_first_order_no,e.online_first_parent_order_no
,e.online_first_sale_time,e.online_first_sale_amt,e.online_first_discount_amt,e.online_first_need_amt
from algorithm_data.HnAllOrder_aa a
join  sourcedata.s03_oms_order_sub c on a.order_no=c.order_no
JOIN  mdata.c_member_info_snap e ON a.member_id=e.member_id and int(e.ECP_ind)<>1
;



-------------second  会员对应的IP/设备信息(最新的时间作为其IP和设备)
SET hive.exec.max.dynamic.partitions=200;
insert overwrite table algorithm_data.HnAllOrder_ac partition(event_id)
SELECT DISTINCT p__ip ip,p__device_id device_id,p_memberid AS member_id, time
,to_date(time) AS date,hour(time) AS hour
,minute(time)  AS minute,event_id
FROM rawdata.event_ros_p8
WHERE event_id IN ('76','81')
;
use algorithm_data;
drop table if exists HnAllOrder_aca;
create table HnAllOrder_aca AS
SELECT a.member_id
,from_unixtime(a.latestUnixTime,"yyyy-MM-dd HH:mm:ss") latestTime
FROM
(SELECT member_id,max(unix_timestamp(time)) latestUnixTime
FROM algorithm_data.HnAllOrder_ac
WHERE event_id='76' AND device_id IS NOT NULL and device_id<>'' and member_id<>''
GROUP BY member_id) a
;

use algorithm_data;
drop table if exists HnAllOrder_acb;
create table HnAllOrder_acb AS
SELECT b.*
FROM  algorithm_data.HnAllOrder_aca a
JOIN (select ip ,device_id,member_id,from_unixtime(unix_timestamp(time),"yyyy-MM-dd HH:mm:ss") time   FROM   algorithm_data.HnAllOrder_ac where event_id='76') b ON a.member_id=b.member_id AND a.latestTime=b.time
;

------------------third  号码前7位
use algorithm_data;
drop table if exists HnAllOrder_b;
create table HnAllOrder_b AS
SELECT a.*
,case  WHEN length(a.recept_phone)<>11 OR  regexp_extract(a.recept_phone,'^[0-9]{3}',0) NOT in (171,130,131,132,133,134,135,136,137,138,139,145,147,149,150,151,152,153,155,156,157,158,159,170,173,175,176,177,178,180,181,182,183,184,185,186,187,188,189) THEN 'Illegal'
       when length(b.mobilearea)=1 OR b.mobilenumber IS NULL THEN 'Doubtful'
       else regexp_extract(b.mobilearea,'([^\x00-\xff]{2})(\d*)(.*)', 1)
 end as  recept_mobilearea_a
FROM algorithm_data.HnAllOrder_ab a
LEFT JOIN idmdata.Phone_Address b ON regexp_extract(a.recept_phone,'^[0-9]{7}',0)=b.mobilenumber
;


use algorithm_data;
drop table if exists HnAllOrder_c;
create table HnAllOrder_c AS
SELECT a.*
,regexp_extract(recept_address_detail,'([^\x00-\xff]*)(\d*)(.*)', 1) as recept_address_same
,case  WHEN length(a.register_Phone)<>11 OR  regexp_extract(a.register_Phone,'^[0-9]{3}',0) NOT in (171,130,131,132,133,134,135,136,137,138,139,145,147,149,150,151,152,153,155,156,157,158,159,170,173,175,176,177,178,180,181,182,183,184,185,186,187,188,189) THEN 'Illegal'
       when length(b.mobilearea)=1 OR b.mobilenumber IS NULL THEN 'Doubtful'
       else regexp_extract(b.mobilearea,'([^\x00-\xff]{2})(\d*)(.*)', 1)
 end as  register_mobilearea_a
FROM algorithm_data.HnAllOrder_b a
LEFT JOIN idmdata.Phone_Address b ON regexp_extract(a.register_Phone,'^[0-9]{7}',0)=b.mobilenumber
;


----------------fourth 会员对应的 设备、IP和相似号码结合，以及会员对应的其他维度
use algorithm_data;
drop table if exists HnAllOrder_d;
create table HnAllOrder_d AS
select a.*
,case when recept_mobilearea_a='上海' then 'ShangHai'
      when recept_mobilearea_a in ('Doubtful','Illegal') then recept_mobilearea_a
      else 'NoShangHai'
 end as recept_mobilearea
,case when register_mobilearea_a='上海' then 'ShangHai'
     when register_mobilearea_a in ('Doubtful','Illegal') then register_mobilearea_a
     else 'NoShangHai'
end as register_mobilearea
,d.cost_price,a.goods_item_amount- case when d.cost_price is null then 0 else d.cost_price end as orderdetailGoodsCost
,c.device_id,c.ip
from algorithm_data.HnAllOrder_c a
LEFT JOIN algorithm_data.HnAllOrder_acb c ON a.member_id=c.member_id
LEFT join mdata.c_goods_info_snap d on a.goods_code=d.goods_sid and abandon_ind="0"
;



use algorithm_data;
drop table if exists algorithm_data.HnAllOrder_e;
create table algorithm_data.HnAllOrder_e AS
select a.member_id
,min(b.acquire_time) first_acquire_time
,max(b.acquire_time) last_acquire_time
,ceil(  avg((unix_timestamp(b.acquire_time)-unix_timestamp(b.create_time))/60)   ) as getCouponSecDif
,avg(a.detailOrder_discount ) avg_detailOrder_discount
,avg(b.coupon_type) coupon_amount
, avg(a.detailOrder_discount ) - avg(b.coupon_type) as avgCouponUse
,count(distinct b.coupon_code) as getCouponNum
,avg(a.orderdetailGoodsCost)  orderdetailGoodsCost
,ROW_NUMBER() OVER(ORDER BY member_id) AS member_seq
from algorithm_data.HnAllOrder_d a
join sourcedata.s01_camp_user_coupon b on a.member_id=b.member_id
group by a.member_id
;


use algorithm_data;
drop table if exists HnAllOrder_spark;
create table HnAllOrder_spark AS
select distinct BIGINT(a.member_id) member_id
,case when a.ip is null then '999999' else a.ip end as ip
,case when a.device_id is null then '999999' else  a.device_id end as device_id
,case when a.recept_address_same is null then '999999' else a.recept_address_same  end as recept_address_same
,case when a.recept_address_detail is null then  '999999' else a.recept_address_detail   end as recept_address_detail
,case when a.recept_name is null then  '999999' else  a.recept_name   end as recept_name
,case when a.recept_phone is null then  '999999' else   a.recept_phone end as recept_phone
,case when a.register_Phone is null then  '999999' else  a.register_Phone  end as register_Phone
,case when a.recept_mobilearea_a is null then  '999999' else  a.recept_mobilearea_a  end as recept_mobilearea_a
,case when a.register_mobilearea_a is null then  '999999' else a.register_mobilearea_a   end as register_mobilearea_a
,case when from_unixtime(unix_timestamp(register_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else from_unixtime(unix_timestamp(register_time),"yyyy-MM-dd HH:mm:ss")   end as  register_time
,case when from_unixtime(unix_timestamp(online_first_sale_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else  from_unixtime(unix_timestamp(online_first_sale_time),"yyyy-MM-dd HH:mm:ss")  end as online_first_sale_time
,case when from_unixtime(unix_timestamp(online_last_sale_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else  from_unixtime(unix_timestamp(online_last_sale_time),"yyyy-MM-dd HH:mm:ss")  end as online_last_sale_time
,float(case when a.online_first_need_amt is null then  '999999' else a.online_first_need_amt   end) as online_first_need_amt
,float(case when a.online_last_need_amt is null then  '999999' else  a.online_last_need_amt  end) as online_last_need_amt
,float(case when a.online_total_need_amt is null then  '999999' else a.online_total_need_amt   end) as online_total_need_amt
,case when from_unixtime(unix_timestamp(b.first_acquire_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else from_unixtime(unix_timestamp(b.first_acquire_time),"yyyy-MM-dd HH:mm:ss")  end as first_acquire_time
,case when from_unixtime(unix_timestamp(b.last_acquire_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else  from_unixtime(unix_timestamp(b.last_acquire_time),"yyyy-MM-dd HH:mm:ss")   end as last_acquire_time
,float(case when b.getCouponSecDif is null then  '999999' else  b.getCouponSecDif   end )as getCouponSecDif
,float(case when b.orderdetailGoodsCost is null then  '999999' else  b.orderdetailGoodsCost  end) as orderdetailGoodsCost
,float(case when b.avg_detailOrder_discount is null then  '999999' else b.avg_detailOrder_discount    end) as avg_detailOrder_discount
,float(case when b.coupon_amount is null then  '999999' else b.coupon_amount    end )as coupon_amount
,float(case when b.avgCouponUse is null then  '999999' else  b.avgCouponUse  end )as avgCouponUse
,float(case when b.getCouponNum is null then  '999999' else b.getCouponNum   end )as getCouponNum
from algorithm_data.HnAllOrder_d a
join algorithm_data.HnAllOrder_e b on a.member_id=b.member_id
;



--当月消费且会员不在历史黄牛模型中
use algorithm_data;
drop table if exists HnAllOrder_sparkCartesianMember;
create table HnAllOrder_sparkCartesianMember AS
select distinct a.member_id,a.year,a.month
from (select a.member_id,a.year,a.month
      from (select  member_id,year(sale_time) year,month(sale_time) month from algorithm_data.HnAllOrder_aa where  year(sale_time)='${hiveconf:Year}'   and month(sale_time)='${hiveconf:Month}')  a
            left join algorithm_data.HnDoubtfulOrder_all b on a.member_id=b.member_id
            where  b.member_id is NULL) a
join  algorithm_data.HnAllOrder_spark b on a.member_id=b.member_id
;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table algorithm_data.allConsumMember PARTITION (year,month)
select distinct a.member_id ,b.member_id,a.year,a.month
from algorithm_data.HnAllOrder_sparkCartesianMember a
join algorithm_data.HnAllOrder_sparkCartesianMember b on a.year=b.year and a.month=b.month
;




use algorithm_data;
drop table if exists HnSaledMemberNum;
create table HnSaledMemberNum AS
select a.*
from (SELECT year(register_time) year, 999 as month,count(DISTINCT member_id) MemberNum
FROM  algorithm_data.HnAllOrder_spark
WHERE year(register_time)=2015
group by year(register_time),999
union all
SELECT year(register_time) year,month(register_time) month,count(DISTINCT member_id) MemberNum
FROM  algorithm_data.HnAllOrder_spark
where year(register_time)>2015 and to_date(register_time)<'2017-12-01'
group by year(register_time) , month(register_time)
union all
SELECT a.year,a.month,count(DISTINCT a.member_id) MemberNum
from (select  year(sale_time) year,month(sale_time) month,member_id  from algorithm_data.HnAllOrder_aa where  to_date(sale_time)>='2017-12-01' ) a
left join algorithm_data.HnDoubtfulOrder_all b on a.member_id=b.member_id
join  algorithm_data.HnAllOrder_spark c on a.member_id=c.member_id
where  b.member_id is NULL
group by a.year,a.month
)a;

