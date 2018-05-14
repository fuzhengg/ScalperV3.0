

-------------first  会员对应的IP/设备信息(最新的时间作为其IP和设备)
--埋点设备号
use algorithm_data;
drop table if exists HnAllOrder_ac;
create table HnAllOrder_ac AS
SELECT DISTINCT p__ip ip,p__device_id device_id,p_memberid AS member_id, time
,to_date(time) AS date,hour(time) AS hour
,minute(time)  AS minute,event_id
FROM rawdata.event_ros_p8
WHERE event_id IN ('76') and p__device_id is not null and p__device_id not in ('')
;

use algorithm_data;
drop table if exists HnAllOrder_aca;
create table HnAllOrder_aca AS
SELECT a.member_id
,from_unixtime(a.latestUnixTime,"yyyy-MM-dd HH:mm:ss") latestTime
FROM
(SELECT member_id,max(unix_timestamp(time)) latestUnixTime
FROM algorithm_data.HnAllOrder_ac
WHERE  device_id IS NOT NULL and device_id<>'' and member_id<>''
GROUP BY member_id) a
;
use algorithm_data;
drop table if exists HnAllOrder_acd;
create table HnAllOrder_acd AS
SELECT b.*
FROM  algorithm_data.HnAllOrder_aca a
JOIN (select ip ,device_id,member_id,from_unixtime(unix_timestamp(time),"yyyy-MM-dd HH:mm:ss") time
      FROM   algorithm_data.HnAllOrder_ac ) b ON a.member_id=b.member_id AND a.latestTime=b.time
;


--订单IP设备号
use algorithm_data;
drop table if exists HnAllOrder_acb;
create table HnAllOrder_acb AS
select a.member_id,b.user_ip as ip ,c.device_no as device_id,a.sale_time
from  (select member_id,parent_order_no,order_no,sale_time   from  mdata.c_order_snap  where online_order_ind='1' and year(sale_time)>=2018 ) a
left join (select  order_parent_no,user_ip from sourcedata.s09_oms_order_parent where  user_ip is not null or trim(user_ip)!='') b on a.parent_order_no=b.order_parent_no
left join (select  order_no,device_no  from mdata.c_order_detail where  device_no is not null or trim(device_no)!='')  c on a.order_no=c.order_no
group by a.member_id,b.user_ip ,c.device_no,a.sale_time
;
use algorithm_data;
drop table if exists HnAllOrder_acc;
create table HnAllOrder_acc AS
select b.*
from (select  member_id,max(sale_time) sale_time   from algorithm_data.HnAllOrder_acb  where device_id is not null or trim(device_id)!='' group by member_id) a
join algorithm_data.HnAllOrder_acb  b on a.member_id=b.member_id and a.sale_time=b.sale_time
where device_id is not NULL
;

--所有会员ID设备号
use algorithm_data;
drop table if exists HnAllOrder_ad;
create table HnAllOrder_ad AS
select a.member_id,COALESCE(b.ip,a.ip) ip ,COALESCE(b.device_id,a.device_id) device_id
from algorithm_data.HnAllOrder_acd a
left join algorithm_data.HnAllOrder_acc b on a.member_id=b.member_id
group by a.member_id,COALESCE(b.ip,a.ip)  ,COALESCE(b.device_id,a.device_id)
;



---- other    1. 所有线上有消费会员基本信息，及收货信息
use algorithm_data;
drop table if exists HnMember_info_a;
create table HnMember_info_a AS
select distinct a.member_id
,case when from_unixtime(unix_timestamp(c.register_time),'yyyy-MM-dd HH:mm:ss')  is null then  '999999' else from_unixtime(unix_timestamp(c.register_time),'yyyy-MM-dd HH:mm:ss')   end as  register_time
,case when b.ip  is null then  '999999' else  b.ip  end ip
,case when b.device_id  is null then  '999999' else  b.device_id  end device_id
,case when a.recept_address_detail  is null then  '999999' else  a.recept_address_detail end recept_address_detail
,case when regexp_extract(a.recept_address_detail,'([^\x00-\xff]*)(\w*)(.*)', 1)  is null then  '999999' else regexp_extract(a.recept_address_detail,'([^\x00-\xff]*)(\w*)(.*)', 1) end  as recept_address_same
,case when a.recept_name  is null then  '999999' else a.recept_name end recept_name
,case when a.recept_phone  is null then  '999999' else  a.recept_phone end recept_phone
,int(c.ECP_ind)  ECP_ind
,case when from_unixtime(unix_timestamp(c.online_last_sale_time),'yyyy-MM-dd HH:mm:ss')  is null then  '999999' else from_unixtime(unix_timestamp(c.online_last_sale_time),'yyyy-MM-dd HH:mm:ss')   end as  online_last_sale_time
,c.online_last_sale_amt,c.online_last_discount_amt,c.online_last_need_amt
,case when from_unixtime(unix_timestamp(c.online_first_sale_time),'yyyy-MM-dd HH:mm:ss')  is null then  '999999' else from_unixtime(unix_timestamp(c.online_first_sale_time),'yyyy-MM-dd HH:mm:ss')   end as  online_first_sale_time
,c.online_first_sale_amt,c.online_first_discount_amt,c.online_first_need_amt
,c.online_total_sale_amt,c.online_total_discount_amt,c.online_total_need_amt
from mdata.c_order_snap a
left join algorithm_data.HnAllOrder_ad b on a.member_id=b.member_id
left JOIN mdata.c_member_info_snap c ON a.member_id=c.member_id
WHERE a.online_order_ind='1' and a.order_va_ind='1'
;

------ other    2.特征合成处理
use algorithm_data;
drop table if exists HnMember_info_check;
create table HnMember_info_check AS
select a.*
,case when ip='999999' or recept_name ='999999' then '999999' else concat(ip,recept_name) end as ipName
,case when ip='999999' or  recept_phone='999999' then '999999' else concat(ip,recept_phone)  end as ipPhone
,case when ip='999999' or  recept_address_same='999999' then '999999' else concat(ip,recept_address_same)  end as ipAddress
,case when ip='999999' or  device_id='999999' then '999999' else concat(ip,device_id)  end as ipDevice
,case when device_id='999999' or  recept_name='999999' then '999999' else concat(device_id,recept_name) end as  deviceName
,case when device_id='999999' or  recept_phone='999999' then '999999' else concat(device_id,recept_phone)  end as devicePhone
,case when device_id='999999' or  recept_address_same='999999' then '999999' else concat(device_id,recept_address_same)  end as deviceAddress
,case when recept_phone='999999' or  recept_address_same='999999' then '999999' else concat(recept_phone,recept_address_same) end as  phoneAddress
,case when recept_phone='999999' or  recept_name='999999' then '999999' else concat(recept_phone,recept_name)  end as phoneName
,case when recept_name='999999' or  recept_address_same='999999' then '999999' else concat(recept_name,recept_address_same)  end as nameAddress
from algorithm_data.HnMember_info_a a
;

------ other    3. 优惠券
use algorithm_data;
drop table if exists HnMember_info_coupon;
create table HnMember_info_coupon AS
select a.member_id,a.couponAcquire_num,a.couponLastAcquire_time,b.coupon_type as couponLastAcquire_type
from (select member_id,count(distinct acquire_time) couponAcquire_num
      ,case when from_unixtime(unix_timestamp(max(acquire_time)),'yyyy-MM-dd HH:mm:ss')  is null then  '999999' else from_unixtime(unix_timestamp(max(acquire_time)),'yyyy-MM-dd HH:mm:ss')   end as  couponLastAcquire_time
      from sourcedata.s01_camp_user_coupon
      group by member_id) a
join sourcedata.s01_camp_user_coupon b on a.member_id=b.member_id and a.couponLastAcquire_time=b.acquire_time
;




------ other    上述3个特征合到一起,且未在黄牛名单中出现
--'${hiveconf:StartDate}'
-- and  to_date(a.sale_time)>='${hiveconf:StartDate}'
--and  to_date(sale_time)>='2018-03-11'
--where  b.circle_memberid is null
use algorithm_data;
drop table if exists HnMember_info_b;
create table HnMember_info_b AS
select a.member_id,b.register_time
,case when b.ip is null or trim(b.ip) in ('',NULL) then '999999' else b.ip end as ip
,case when b.device_id is null or trim(b.device_id) in ('',NULL)   then '999999' else b.device_id end as device_id
,case when b.recept_address_detail is null  or trim(b.recept_address_detail) in ('',NULL)  then '999999' else b.recept_address_detail end as recept_address_detail
,case when b.recept_address_same is null or trim(b.recept_address_same) in ('',NULL)   then '999999' else b.recept_address_same end as recept_address_same
,case when b.recept_name   is null  or trim(b.recept_name) in ('',NULL)  then '999999' else b.recept_name end as recept_name
,case when b.recept_phone  is null or trim(b.recept_phone) in ('',NULL)   then '999999' else b.recept_phone end as recept_phone
,b.ipPhone,b.ipName,b.ipAddress,b.ipDevice,b.deviceName,b.devicePhone,b.deviceAddress,b.phoneAddress,b.phoneName,b.nameAddress
,case when b.ecp_ind is null then 999999 else b.ecp_ind end as ecp_ind
,case when b.online_last_sale_time is null  or trim(b.online_last_sale_time) in ('',NULL)  then '999999' else b.online_last_sale_time end as online_last_sale_time
,case when b.online_last_sale_amt is null then 0.0 else b.online_last_sale_amt end as online_last_sale_amt
,case when b.online_last_discount_amt is null then 0.0 else b.online_last_discount_amt end as online_last_discount_amt
,case when b.online_last_need_amt is null then 0.0 else b.online_last_need_amt end as online_last_need_amt
,case when b.online_first_sale_time is null then 0.0 else b.online_first_sale_time end as online_first_sale_time
,case when b.online_first_sale_amt is null then 0.0 else b.online_first_sale_amt end as online_first_sale_amt
,case when b.online_first_discount_amt is null then 0.0 else b.online_first_discount_amt end as online_first_discount_amt
,case when b.online_first_need_amt is null then 0.0 else b.online_first_need_amt end as online_first_need_amt
,case when b.online_total_sale_amt is null then 0.0 else b.online_total_sale_amt end as online_total_sale_amt
,case when b.online_total_discount_amt is null then 0.0 else b.online_total_discount_amt end as online_total_discount_amt
,case when b.online_total_need_amt is null then 0.0 else b.online_total_need_amt end as online_total_need_amt
,case when c.couponAcquire_num is NULL THEN 0 else c.couponAcquire_num end as couponAcquire_num
,case when c.couponLastAcquire_time is NULL or trim(c.couponLastAcquire_time)='' THEN '999999' else c.couponLastAcquire_time end as couponLastAcquire_time
,case when c.couponLastAcquire_type is NULL THEN 0.0 else c.couponLastAcquire_type end as couponLastAcquire_type
from (select * from mdata.c_order_snap WHERE online_order_ind='1' and order_va_ind='1' and member_id is not null and  to_date(sale_time)>='${hiveconf:StartDate}')  a
join  algorithm_data.HnMember_info_check b  ON a.member_id=b.member_id and a.recept_name=b.recept_name and a.recept_address_detail=b.recept_address_detail and a.recept_phone=b.recept_phone
left join algorithm_data.HnMember_info_coupon c on a.member_id=c.member_id
group by a.member_id,b.register_time,b.ip,b.device_id,b.recept_address_detail,b.recept_address_same,b.recept_name  ,b.recept_phone
,b.ipPhone,b.ipName,b.ipAddress,b.ipDevice,b.deviceName,b.devicePhone,b.deviceAddress,b.phoneAddress,b.phoneName,b.nameAddress
,b.ecp_ind,b.online_last_sale_time,b.online_last_sale_amt,b.online_last_discount_amt,b.online_last_need_amt,b.online_first_sale_time,b.online_first_sale_amt,b.online_first_discount_amt,b.online_first_need_amt,b.online_total_sale_amt,b.online_total_discount_amt,b.online_total_need_amt
,c.couponAcquire_num,c.couponLastAcquire_time,c.couponLastAcquire_type
;


--找出合成特征对应的会员量
use algorithm_data;
drop table if exists HnMember_info;
create table HnMember_info AS
select a.*
,case when b.ipPhone='999999' then   case when a.device_id='999999' then phonememnum else devmemnum end  else  b.ipPhoneNum end as ipPhoneNum
,case when c.ipName='999999' then    case when a.device_id='999999' then phonememnum else devmemnum end  else   c.ipNameNum end as ipNameNum
,case when d.ipAddress='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  d.ipAddressNum end as ipAddressNum
,case when f.deviceName='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else   f.deviceNameNum end as deviceNameNum
,case when g.devicePhone='999999' then  case when a.device_id='999999' then phonememnum else devmemnum end  else g.devicePhoneNum  end as devicePhoneNum
,case when h.deviceAddress='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else h.deviceAddressNum end as deviceAddressNum
,case when i.device_id='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  i.devmemnum end as devmemnum
,case when j.recept_phone='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  j.phonememnum end as phonememnum
,case when k.recept_address_same='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  k.sameaddressmemnum end as sameaddressmemnum
,case when l.ip='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  l.ipmemnum end as ipmemnum
,case when m.recept_name='999999' then case when a.device_id='999999' then phonememnum else devmemnum end else  m.namememnum end as namememnum
from HnMember_info_b a
left join (select ipPhone,count(distinct member_id)  ipPhoneNum from algorithm_data.HnMember_info_check group by ipPhone) b on a.ipPhone=b.ipPhone
left join (select ipName,count(distinct member_id)  ipNameNum from algorithm_data.HnMember_info_check group by ipName) c on a.ipName=c.ipName
left join (select ipAddress,count(distinct member_id)  ipAddressNum from algorithm_data.HnMember_info_check group by ipAddress) d on a.ipAddress=d.ipAddress
left join (select deviceName,count(distinct member_id)  deviceNameNum from algorithm_data.HnMember_info_check group by deviceName) f on a.deviceName=f.deviceName
left join (select devicePhone,count(distinct member_id)  devicePhoneNum from algorithm_data.HnMember_info_check group by devicePhone) g on a.devicePhone=g.devicePhone
left join (select deviceAddress,count(distinct member_id)  deviceAddressNum from algorithm_data.HnMember_info_check group by deviceAddress) h on a.deviceAddress=h.deviceAddress
left join (select device_id,count(distinct member_id)  devmemnum from algorithm_data.HnMember_info_check group by device_id) i on a.device_id=i.device_id
left join (select recept_phone,count(distinct member_id)  phonememnum from algorithm_data.HnMember_info_check group by recept_phone) j on a.recept_phone=j.recept_phone
left join (select recept_address_same,count(distinct member_id)  sameaddressmemnum from algorithm_data.HnMember_info_check group by recept_address_same) k on a.recept_address_same=k.recept_address_same
left join (select ip,count(distinct member_id)  ipmemnum from algorithm_data.HnMember_info_check group by ip) l on a.ip=l.ip
left join (select recept_name,count(distinct member_id)  namememnum from algorithm_data.HnMember_info_check group by recept_name) m on a.recept_name=m.recept_name
;


use algorithm_data;
drop table if exists HnOrder_info;
create table HnOrder_info as
select a.*
from mdata.c_order_snap a
join algorithm_data.HnMember_info b on a.member_id=b.member_id and a.recept_address_detail=b.recept_address_detail and a.recept_name=b.recept_name and a.recept_phone=b.recept_phone
WHERE a.online_order_ind='1' and a.order_va_ind='1'
;



use algorithm_data;
drop table if exists HnSaledMemberNum;
create table HnSaledMemberNum AS
select a.*
from (SELECT year(register_time) year, 99  as month,count(DISTINCT member_id) MemberNum
FROM  mdata.c_member_info_snap
WHERE year(register_time)<=2017
group by year(register_time),99
union all
SELECT year(sale_time) year,month(sale_time) month,count(DISTINCT member_id) MemberNum
FROM  mdata.c_order_snap
where year(sale_time)>2017
group by year(sale_time) , month(sale_time)
)a;






--others 白名单 (收货号码、注册号码在白名单中，或者会员为钻石会员)
use algorithm_data;
drop table if exists algorithm_data.Hn_MemberwhiteList;
create table algorithm_data.Hn_MemberwhiteList AS
select distinct a.whitephone,a.member_id as whitemember
from (select  a.whitephone,b.member_id
FROM algorithm_data.Hn_PhonewhiteList a
join mdata.c_order_snap b on a.whitephone=b.recept_phone
union all
select  a.whitephone,b.member_id
from algorithm_data.Hn_PhonewhiteList a
join mdata.c_member_info_snap b on a.whitephone=b.mobile
union all
select  mobile as whitephone,member_id
from mdata.c_member_info_snap
where int(member_level_id)=40
) a;
