
--use algorithm_data;
--drop table if exists algorithm_data.test_HnStatics;
--create table algorithm_data.test_HnStatics AS
--SELECT *
--FROM  algorithm_data.hnallorder_circleresult
--WHERE  Year=2017 AND Month=12 AND Day=999
--;



use algorithm_data;
drop table if exists algorithm_data.HnAllOrder_e;
create table algorithm_data.HnAllOrder_e AS
select distinct  c.circle_id,c.circle_size,c.circle_memberid,c.device_id
,b.acquire_time
,case when from_unixtime(unix_timestamp(a.sale_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else from_unixtime(unix_timestamp(a.sale_time),"yyyy-MM-dd HH:mm:ss")   end as  sale_time
,a.need_money as order_need_money,a.ticket_money as order_ticket_money
,case when a.recept_mobilearea_a is null then  '999999' else  a.recept_mobilearea_a  end as recept_mobilearea_a
,case when a.register_mobilearea_a is null then  '999999' else a.register_mobilearea_a   end as register_mobilearea_a
,case when from_unixtime(unix_timestamp(a.register_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else from_unixtime(unix_timestamp(a.register_time),"yyyy-MM-dd HH:mm:ss")   end as  register_time
from  algorithm_data.test_HnStatics c
join algorithm_data.HnAllOrder_d a on c.circle_memberid=a.member_id
left join sourcedata.s01_camp_user_coupon b on a.member_id=b.member_id
;


--注册时间评分
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_aa;
create table algorithm_data.test_HnStatics_aa AS
select a.circle_id,a.circle_size
,log10(a.circle_size)/((count(distinct device_id)+count(distinct register_date))* case when avg(register_time_std)=0 then 1 else avg(register_time_std) end ) as register_Score
from (select circle_id,circle_size,to_date(register_time) register_date
,count(distinct circle_memberid) as circle_membernum
,case when count(distinct circle_memberid)=1 then 9999
      when count(distinct circle_memberid)>1 then stddev_pop(3600*hour(register_time)+60*minute(register_time)+second(register_time))
      end as register_time_std
from algorithm_data.HnAllOrder_e
where register_time is not null
group by circle_id,circle_size,to_date(register_time) ) a
join algorithm_data.HnAllOrder_e b on a.circle_id=b.circle_id and a.circle_size=b.circle_size
group by a.circle_id,a.circle_size
;
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_a;
create table algorithm_data.test_HnStatics_a AS
select b.circle_id,b.circle_size
,(b.register_Score-a.register_Score_min)/(a.register_Score_max-a.register_Score_min) as register_Score
from(select min(register_Score) register_Score_min,max(register_Score) register_Score_max
     from algorithm_data.test_HnStatics_aa
     ) a
join algorithm_data.test_HnStatics_aa b on 1=1
;

--统计评分
use algorithm_data;
drop table if exists HnAllOrder_info;
create table HnAllOrder_info AS
select distinct a.member_id,b.ip,b.device_id
,regexp_extract(a.recept_address_detail,'([^\x00-\xff]*)(\d*)(.*)', 1) as recept_address_same
,a.recept_address_detail,a.recept_name,d.recept_phone
,int(c.ECP_ind)  ECP_ind
from mdata.c_order_snap a
join algorithm_data.HnAllOrder_acb b on a.member_id=b.member_id
JOIN mdata.c_member_info_snap c ON a.member_id=c.member_id
join sourcedata.s03_oms_order_sub d on a.order_no=d.order_no
WHERE a.online_order_ind='1'
;



use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_device;
create table algorithm_data.test_HnStatics_ba AS
select device_id,count(distinct member_id) as  memberNum
from algorithm_data.HnAllOrder_acb
group by device_id
;
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_ip;
create table algorithm_data.test_HnStatics_bb AS
select ip,count(distinct member_id) as  memberNum
from algorithm_data.HnAllOrder_acb
group by ip
;
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_addressSame;
create table algorithm_data.test_HnStatics_addressSame AS
select regexp_extract(a.recept_address_detail,'([^\x00-\xff]*)(\d*)(.*)', 1) as recept_address_same
,count(distinct member_id) as  memberNum
from  mdata.c_order_snap
WHERE a.online_order_ind='1'
group by regexp_extract(a.recept_address_detail,'([^\x00-\xff]*)(\d*)(.*)', 1)
;



--消费金额
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_ca;
create table algorithm_data.test_HnStatics_ca AS
select circle_id,circle_size
,sale_date
,avg(order_needMoney_per) as order_needMoney_per_Avg
,stddev_pop(order_needMoney_per) order_needMoney_per_std
from (select circle_id,circle_size,to_date(sale_time) sale_date
      ,circle_memberid,avg(order_need_money/(order_ticket_money+order_need_money)) as order_needMoney_per
      from algorithm_data.HnAllOrder_e
      group by circle_id,circle_size,to_date(sale_time),circle_memberid ) a
group by circle_id,circle_size,sale_date
;
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_cb;
create table algorithm_data.test_HnStatics_cb AS
select circle_id,circle_size,pow(2,avg(order_needMoney_per_Avg)-avg(order_needMoney_per_std) )saleMoney_Score
from algorithm_data.test_HnStatics_ca
group by  circle_id,circle_size
;

use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_c;
create table algorithm_data.test_HnStatics_c AS
select b.circle_id,b.circle_size
,(b.saleMoney_Score-a.saleMoney_Score_min)/(a.saleMoney_Score_max-a.saleMoney_Score_min) as saleMoney_Score
from(select min(saleMoney_Score) saleMoney_Score_min,max(saleMoney_Score) saleMoney_Score_max
     from algorithm_data.test_HnStatics_cb
     ) a
join algorithm_data.test_HnStatics_cb b on 1=1
;


--优惠券领取时间和消费时间
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_da;
create table algorithm_data.test_HnStatics_da AS
select circle_id,circle_size
,(avg(acquire_time_stdd)+avg(sale_time_stdd)) / avg(acquireCoupon_MemberDif)   as coupon_Score
from (select circle_id,circle_size,to_date(acquire_time) acquire_date
      ,abs(count(distinct acquire_time)-count(distinct circle_memberid)) as acquireCoupon_MemberDif
      ,count(distinct circle_memberid)/stddev_pop(3600*hour(acquire_time)+60*minute(acquire_time)+second(acquire_time))  acquire_time_stdd
      ,count(distinct circle_memberid)/stddev_pop(3600*hour(sale_time)+60*minute(sale_time)+second(sale_time))  sale_time_stdd
      from algorithm_data.HnAllOrder_e
      where acquire_time is not NULL
      group by circle_id,circle_size,to_date(acquire_time)
      ) a
group by circle_id,circle_size
;
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_d;
create table algorithm_data.test_HnStatics_d AS
select b.circle_id,b.circle_size
,(b.coupon_Score-a.coupon_Score_min)/(a.coupon_Score_max-a.coupon_Score_min) as coupon_Score
from(select min(coupon_Score) coupon_Score_min,max(coupon_Score) coupon_Score_max
     from algorithm_data.test_HnStatics_da
     ) a
join algorithm_data.test_HnStatics_da b on 1=1
;


use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_Score_a;
create table algorithm_data.test_HnStatics_Score_a AS
select a.circle_id,a.circle_size
,a.register_Score,c.saleMoney_Score,d.coupon_Score
,0.2*a.register_Score+0.3*d.coupon_Score+0.5*c.saleMoney_Score  as Score
from algorithm_data.test_HnStatics_a a
join algorithm_data.test_HnStatics_c c on a.circle_id=c.circle_id and a.circle_size=c.circle_size
join algorithm_data.test_HnStatics_d d on c.circle_id=d.circle_id and c.circle_size=d.circle_size
;


use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_Score;
create table algorithm_data.test_HnStatics_Score AS
select b.circle_id,b.circle_size
,(b.Score-a.Score_min)/(a.Score_max-a.Score_min) as Score
,b.register_Score,b.saleMoney_Score,b.coupon_Score
from(select min(Score) Score_min,max(Score) Score_max
     from algorithm_data.test_HnStatics_Score_a
     ) a
join algorithm_data.test_HnStatics_Score_a b on 1=1
;

insert  overwrite table algorithm_data.hnCircleScore PARTITION (Year=2016,Month=12,Day=31,category='all')
select a.circle_id,a.circle_size,a.score,b.circle_memberid
,case when from_unixtime(unix_timestamp(b.register_time),"yyyy-MM-dd HH:mm:ss")  is null then  '999999' else from_unixtime(unix_timestamp(b.register_time),"yyyy-MM-dd HH:mm:ss")   end as  register_time
,b.ip,b.device_id,b.recept_address_same
,b.recept_address_detail,b.recept_name,b.recept_phone,b.register_phone,a.register_score,a.salemoney_score,a.coupon_score
from algorithm_data.test_HnStatics_Score a
join algorithm_data.test_HnStatics b on a.circle_id=b.circle_id and a.circle_size=b.circle_size
;


