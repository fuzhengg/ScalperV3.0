

--1找会员信息
select *
from  algorithm_data.HnMember_info_check
where member_id in ('100000000010887')
;

--2 根据会员信息找是否在黑名单
select *
from algorithm_data.HnDoubtfulOrder_all
where member_id in ('100000000010887')
or recept_phone in ('13817125756','13671527115')
or recept_name in ('Phil Gu','乐洒脱','乐萍')
or device_id in ('B8BCB1A8-6FF8-4CCD-B472-21E51EA5D900')
;
or recept_address_detail in ('上海市 市辖区 黄浦区 四川南路26号')
;
--哪一类中
select *
from algorithm_data.hnCircleScore
where  circle_memberid in ('100000000010887','100000000976390')
or device_id in ('B8BCB1A8-6FF8-4CCD-B472-21E51EA5D900','BF35D1B1-F3DA-4BB8-B311-B2076F3D888D')
;



--3. 找出其社交圈信息
select *
from algorithm_data.HnDoubtfulOrder_all
where circle_id in ('100000000230200')
;

select a.circle_id,a.circle_size,a.score,b.*
from (select *
from algorithm_data.hnCircleScore where circle_id in ('100000000230200') ) a
join algorithm_data.HnMember_info_check b on a.circle_memberid=b.member_id
;


--插入白名单
insert  into table algorithm_data.HnDoubtfulOrder_all_back partition (day='2018-03-26' ,special='black')
select '100000000010887' as member_id,'13671527115' as recept_phone,'乐萍' as recept_name,null as recept_address_detail
,null as ip,'B8BCB1A8-6FF8-4CCD-B472-21E51EA5D900' as device_id,null as register_time,null as circle_id ,null as circle_size,null as level
from algorithm_data.test2
limit 1
;
select * from algorithm_data.HnDoubtfulOrder_all_back where special='black'


--更新线上数据
use algorithm_data;
drop table if exists algorithm_data.test3;
create table algorithm_data.test3 AS
select a.*
 from ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where day='2018-03-28' and special='normal') a
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') b on a.recept_phone=b.recept_phone
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') c on a.member_id=c.member_id
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') d on a.recept_address_detail=d.recept_address_detail
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') e on a.device_id=e.device_id
 where (b.recept_phone is null or b.recept_phone in ('')) and  (c.member_id in ('') or c.member_id is null)
and  (d.recept_address_detail in ('') or d.recept_address_detail is null)   and  (e.device_id in ('') or e.device_id is null)
 ;
select * from algorithm_data.test3 where recept_phone  in ('15640984113','13022494349');


use algorithm_data;
drop table if exists algorithm_data.HnDoubtfulOrder_all;
create table algorithm_data.HnDoubtfulOrder_all AS
select member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,string(circle_id) circle_id,string(circle_size) circle_size, level
from algorithm_data.test3
group by member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,string(circle_id),string(circle_size), level,day,special
;
--测试  用开始的代码

url='http://10.201.32.182:7060/dp-compassServer/api/v1/order/redis/aorder'
curl $url




--else  统计信息
--初始筛选剩下会员个数
select count(distinct circle_memberid)
from algorithm_data.hnCircleScore
where  category='20180322nonRealTime'
;
select count(distinct circle_memberid)
from algorithm_data.hnCircleScore
where category='nonRealTime'
;


select count(distinct a.circle_memberid)
from (select circle_memberid from algorithm_data.hnCircleScore  where  category='20180322nonRealTime') a
join (select circle_memberid from algorithm_data.hnCircleScore  where   category='nonRealTime') b on a.circle_memberid=b.circle_memberid
;


SELECT a.*
FROM algorithm_data.HnMember_info a
JOIN (select a.circle_memberid
from (select circle_memberid from algorithm_data.hnCircleScore  where  category='nonRealTime') a
left join (select circle_memberid from algorithm_data.hnCircleScore  where   category='20180322nonRealTime') b on a.circle_memberid=b.circle_memberid
where b.circle_memberid is NULL and a.circle_memberid is not NULL
GROUP BY a.circle_memberid) b ON bigint(a.member_id)=bigint(b.circle_memberid)
;

select recept_address_same,count(distinct member_id)
from algorithm_data.HnMember_info_b
group by recept_address_same
;



--相似地址对应的会员量
select b.recept_address_same,count(distinct a.circle_memberid) circle_membernum
from (select *
from algorithm_data.hnCircleScore where category='20180322nonRealTime' ) a
left join algorithm_data.HnMember_info b on a.circle_memberid=b.member_id and a.device_id=b.device_id and a.ip=b.ip and a.recept_name=b.recept_name
group by b.recept_address_same
;
--'.*四川南路.*' 对应的详细信息
select a.circle_id,a.circle_size,a.score,b.*
from (select *
from algorithm_data.hnCircleScore where category='20180322nonRealTime' ) a
join algorithm_data.HnMember_info_check b on a.circle_memberid=b.member_id and a.device_id=b.device_id and a.ip=b.ip and a.recept_name=b.recept_name and b.recept_address_same in ('上海市 市辖区 黄浦区 四川南路','上海市 市辖区 黄浦区 百联全渠道有限公司黄浦区四川南路','上海市 市辖区 黄浦区 友谊大厦涉外商务楼四川南路')
;

--'.*四川南路.*' 对应的社交圈详细信息
select a.circle_id,a.circle_size,a.circle_memberid,a.score,a.year,a.month,a.day,b.*
from (select *
from algorithm_data.hnCircleScore where category='20180322nonRealTime'  and circle_id in('100000000000360','100000000010447','100000000001042')) a
left  join algorithm_data.HnMember_info_check b on a.circle_memberid=b.member_id and a.device_id=b.device_id and a.ip=b.ip and a.recept_name=b.recept_name
;







--进入模型会员个数
select year(register_time) register_time,count(distinct member_id)
from algorithm_data.HnMember_info_b
group by year(register_time)
;

--最终模型筛选得到的会员个数
select count(distinct member_id)
from algorithm_data.HnMember_info
where member_id in ()
;




















hive -e "
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select a.*,b.*
from (SELECT * FROM  algorithm_data.hnCircleScore WHERE  year=2018 AND month=2 AND day=12  and to_date(register_time)>'2018-02-01'  ) a
join (select a.*  from mdata.c_order_snap a  WHERE a.online_order_ind='1' and a.order_va_ind='1' ) b on a.circle_memberid=b.member_id
left join algorithm_data.Hn_MemberwhiteList c on a.circle_memberid =c.whitemember
where c.whitemember is  null
;" >test.txt
/opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/txt2xlsx.py  test.txt test.xlsx







--去除ECP会员ID对应的姓名
use algorithm_data;
drop table if exists algorithm_data.test21;
create table algorithm_data.test21 AS
select  a.member_id,a.recept_name,count(distinct b.order_no) order_num
from (SELECT distinct member_id,recept_name  FROM algorithm_data.HnAllOrder_test2 WHERE level='非常怀疑' and recept_name is not null and trim(recept_name) not in ('') and length(recept_name)>1) a
join (select distinct member_id,recept_name,order_no from  mdata.c_order_snap where online_order_ind='1' and order_va_ind='1') b on a.member_id=b.member_id  and a.recept_name =b.recept_name
group by  a.member_id,a.recept_name
;

use algorithm_data;
drop table if exists algorithm_data.test22;
create table algorithm_data.test22 AS
select member_id,recept_name,order_num
,ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_num desc) AS rn
from algorithm_data.test21
;

use algorithm_data;
drop table if exists algorithm_data.test23;
create table algorithm_data.test23 AS
select distinct recept_name
,regexp_extract(recept_name,'([^\x00-\xff])(先生|女士|小姐)(.*)', 1) regexp_name_a
from algorithm_data.test22
where rn=1 and regexp_extract(recept_name,'([^\x00-\xff])(先生|女士|小姐)(.*)', 1) is null
;


use algorithm_data;
drop table if exists algorithm_data.test_HnStatics;
create table algorithm_data.test_HnStatics AS
select a.circle_id,a.circle_size,a.score,a.circle_memberid,a.register_time,a.device_id,a.ip,a.order_num,a.registertime_score,a.deviceidscore,a.saletime_score,a.needmoney_score,a.discountmoney_score
,b.*
from (SELECT * FROM  algorithm_data.hnCircleScore WHERE  year=2017 AND month=3 AND day=31  and  circle_id in ('100000003338577') ) a
join (select a.*  from mdata.c_order_snap a  WHERE a.online_order_ind='1' and a.order_va_ind='1' ) b on a.circle_memberid=b.member_id
left join algorithm_data.Hn_MemberwhiteList c on a.circle_memberid =c.whitemember
where c.whitemember is  null
;

select circle_id,circle_size,device_id,count(distinct circle_memberid) circle_memberNum
from algorithm_data.test_HnStatics
group by circle_id,circle_size,device_id
;

select circle_id,circle_size,recept_name,count(distinct circle_memberid) circle_memberNum
from algorithm_data.test_HnStatics
where device_id='999999'
group by circle_id,circle_size,recept_name
;






--test
use algorithm_data;
drop table if exists algorithm_data.test_HnStatics;
create table algorithm_data.test_HnStatics AS
SELECT a.member_id AS all_member_id
,b.member_id AS saled_member_id
,c.member_id AS inModule_member_id
,d.member_id AS Moduled_member_id
,a.cust_type,a.cert_type,a.cert_code,a.black_flag,a.member_level_id,a.ECP_ind,a.channel_id
,d.*
FROM mdata.c_member_info_snap a
LEFT JOIN mdata.c_order_snap b ON a.member_id=b.member_id
LEFT JOIN algorithm_data.HnAllOrder_spark c  ON a.member_id=c.member_id
LEFT JOIN algorithm_data.HnDoubtfulOrder_all d ON a.member_id=d.member_id
;
--会员量分布
select
count(distinct all_member_id)-count(distinct saled_member_id) as noSaled_member_num
,count(distinct saled_member_id) saled_member_num
,count(distinct inModule_member_id) inModule_member_num
,count(distinct Moduled_member_id) Moduled_member_num
,b.ECP_saled_member_num,c.NECP_saled_member_num
from algorithm_data.test_HnStatics a
join (select count(distinct saled_member_id) as ECP_saled_member_num from algorithm_data.test_HnStatics where int(ECP_ind)=1) b  on 1=1
join (select count(distinct saled_member_id) as NECP_saled_member_num from algorithm_data.test_HnStatics where int(ECP_ind)=0) c on 1=1
group by b.ECP_saled_member_num,c.NECP_saled_member_num
;
--黄牛会员金银牌等级分布
select
member_level_id
,count(distinct Moduled_member_id)  Moduled_member_num
,count(distinct all_member_id)  all_member_num
from algorithm_data.test_HnStatics
group by member_level_id
;
--黄牛钻石卡原因
select a.*
from algorithm_data.HnDoubtfulOrder_all a
join (select distinct Moduled_member_id from algorithm_data.test_HnStatics  where  int(member_level_id)=40 )b on a.member_id=b.Moduled_member_id
;


use algorithm_data;
drop table if exists algorithm_data.test_HnStatics_a;
create table algorithm_data.test_HnStatics_a AS
SELECT *
FROM algorithm_data.hnallorder_circleResult
WHERE  circle_id='100000001800855'
;


select count(distinct circle_memberid)
from (SELECT distinct recept_name,recept_address_detail FROM algorithm_data.test_HnStatics_a WHERE member_id in ('100000002275810','100000002276079','100000002278245','100000002278618','100000002279554','100000002279836','100000002286391','100000002289494','100000002295283','100000002362377')) a
join  algorithm_data.test_HnStatics_a b on a.recept_name=b.recept_name and a.recept_address_detail=b.recept_address_detail
;

select b.*
from (SELECT distinct recept_name,recept_address_detail FROM algorithm_data.test_HnStatics_a WHERE member_id in ('100000002275810','100000002276079','100000002278245','100000002278618','100000002279554','100000002279836','100000002286391','100000002289494','100000002295283','100000002362377')) a
join  algorithm_data.test_HnStatics_a b on a.recept_name=b.recept_name
;
select b.*
from (SELECT distinct circle_memberid FROM algorithm_data.test_HnStatics_a WHERE member_id in ('100000002275810','100000002276079','100000002278245','100000002278618','100000002279554','100000002279836','100000002286391','100000002289494','100000002295283','100000002362377')) a
join  algorithm_data.HnAllOrder_ab b on a.circle_memberid=b.member_id
;

select *
from (SELECT distinct recept_address_detail FROM algorithm_data.test_HnStatics_a WHERE member_id in ('100000002275810','100000002276079','100000002278245','100000002278618','100000002279554','100000002279836','100000002286391','100000002289494','100000002295283','100000002362377')) a
join  algorithm_data.test_HnStatics_a b on a.recept_address_detail=b.recept_address_detail
;
select DISTINCT member_id,order_no,order_source_code,goods_name,goods_salesum
,sale_time,pay_time,need_money
,recept_name,b.recept_address_detail,recept_phone,register_phone
,cert_type,cert_code,register_time,login_code
,online_last_order_no, online_last_parent_order_no, online_last_sale_time, online_last_sale_amt
, online_last_discount_amt, online_last_need_amt, online_total_sale_amt, online_total_discount_amt
, online_total_need_amt, online_first_order_no, online_first_parent_order_no, online_first_sale_time
, online_first_sale_amt, online_first_discount_amt, online_first_need_amt
from (SELECT distinct recept_address_detail FROM algorithm_data.test_HnStatics_a WHERE member_id in ('100000002275810','100000002276079','100000002278245','100000002278618','100000002279554','100000002279836','100000002286391','100000002289494','100000002295283','100000002362377')) a
join  algorithm_data.HnAllOrder_ab b on a.recept_address_detail=b.recept_address_detail
;
select DISTINCT member_id,order_no,order_source_code,goods_name,goods_salesum
,sale_time,pay_time,need_money
,recept_name,recept_address_detail,recept_phone,register_phone
,cert_type,cert_code,register_time,login_code
,online_last_order_no, online_last_parent_order_no, online_last_sale_time, online_last_sale_amt
, online_last_discount_amt, online_last_need_amt, online_total_sale_amt, online_total_discount_amt
, online_total_need_amt, online_first_order_no, online_first_parent_order_no, online_first_sale_time
, online_first_sale_amt, online_first_discount_amt, online_first_need_amt,FLOAT(online_total_discount_amt)/FLOAT(online_last_need_amt) AS online_last_amt_per
from  algorithm_data.HnAllOrder_ab
where  member_id in ('100000000125313','100000000125556','100000000125559','100000000132657','100000000132686','100000000132690','100000000133037','100000000133042','100000000133045','100000000133048','100000000133054','100000000124068','100000000124071','100000000124336','100000000124338','100000000125108','100000000134078','100000000134083','100000000134265','100000000134276','100000000134304','100000000134311','100000000134316','100000000134322','100000000134330','100000000134333','100000000134337','100000000134376','100000000134386','100000000134399','100000000134404','100000000134415','100000000134424','100000000134430','100000000134457','100000000134460','100000000134465','100000000134489','100000000134500','100000000134518','100000000134522','100000000134537','100000000134544','100000000134551','100000000134568','100000000134573','100000000134598','100000000134604','100000000134611','100000000134623','100000000134626','100000000133060','100000000133069','100000000133075','100000000133091','100000000133116','100000000133118','100000000133129','100000000133134','100000000133139','100000000133143','100000000133149','100000000133158','100000000133164','100000000133166','100000000133171','100000000133179','100000000133184','100000000133188','100000000133193','100000000133199','100000000133214','100000000133219','100000000133232','100000000133238','100000000133245','100000000133267','100000000133278','100000000133300','100000000133306','100000000133363','100000000118129','100000000118025','100000000118133','100000000117892','100000000112633','100000000118109','100000000112870','100000000112879','100000000132616','100000000132623','100000000132624','100000000132633','100000000132843','100000000132858','100000000132862','100000000132871','100000000132875','100000000132882','100000000132883','100000000132886','100000000132891','100000000132893','100000000132900','100000000132918','100000000132923','100000000132928','100000000132934','100000000132943','100000000132967','100000000132972','100000000132982','100000000132991','100000000132995','100000000133000','100000000133009','100000000133016','100000000133020','100000000133025','100000000133029','100000000132582','100000000117800','100000000117814','100000000117898','100000000117924','100000000117963','100000000118348','100000000119231','100000000119267','100000000119290','100000000119366','100000000119380','100000000119389','100000000119426','100000000119440','100000000119450','100000000119459','100000000119485','100000000119492','100000000119506','100000000119524','100000000119530','100000000119539','100000000119551','100000000119593','100000000119607','100000000119618','100000000119644','100000000124958','100000000125098','100000000125100','100000000125431','100000000125661','100000000125667')
;



use algorithm_data;
drop table if exists algorithm_data.test_HnStatics;
create table algorithm_data.test_HnStatics AS
SELECT *
FROM  algorithm_data.hnCircleScore
WHERE  year=2017 AND month=7 AND day=31
;

select count(distinct member_id) from algorithm_data.HnOrder_info;
select count(distinct member_id) from algorithm_data.HnMember_info;





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
,log10(a.circle_size)/((count(distinct device_id)+count(distinct register_date))*avg(register_time_std)) as register_Score
from (select circle_id,circle_size,to_date(register_time) register_date
,count(distinct circle_memberid) as circle_membernum
,(stddev_pop(3600*hour(register_time)+60*minute(register_time)+second(register_time)))/count(distinct circle_memberid)  register_time_std
from algorithm_data.HnAllOrder_e
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


--号码评分
--use algorithm_data;
--drop table if exists algorithm_data.test_HnStatics_ba;
--create table algorithm_data.test_HnStatics_ba AS
--select circle_id,circle_size
--,log2(2+ count(distinct register_mobilearea_a) ) / log2(2+ count(distinct register_mobilearea_a)) as phone_Score
--from algorithm_data.HnAllOrder_e
--group by circle_id,circle_size
--;
--use algorithm_data;
--drop table if exists algorithm_data.test_HnStatics_b;
--create table algorithm_data.test_HnStatics_b AS
--select b.circle_id,b.circle_size
--,(b.phone_Score-a.phone_Score_min)/(a.phone_Score_max-a.phone_Score_min) as phone_Score
--from(select min(phone_Score) phone_Score_min,max(phone_Score) phone_Score_max
--     from algorithm_data.test_HnStatics_ba
--     ) a
--join algorithm_data.test_HnStatics_ba b on 1=1
--;

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

insert insert overwrite table algorithm_data.hnCircleScore PARTITION (year=2015,month=999,day=999)
select a.Score,a.register_Score,a.saleMoney_Score,a.coupon_Score
,b.*
from algorithm_data.test_HnStatics_Score a
join algorithm_data.test_HnStatics b on a.circle_id=b.circle_id and a.circle_size=b.circle_size
;




--use algorithm_data;
--drop table if exists HnAllOrder_test2;
--create table HnAllOrder_test2 as
--SELECT distinct String(circle_memberid) as  member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time
--,circle_id,circle_size, '严重怀疑' as level
--FROM algorithm_data.hnallorder_circleResult
--where year<=$iniYear and month<=$iniMonth
--;


--将合成特征对应的会员量进行标准化
use algorithm_data;
drop table if exists HnMember_info_d;
create table HnMember_info_d AS
select a.ipPhoneNumAVG,a.ipPhoneNumSTD,b.ipNameNumAVG,b.ipNameNumSTD,c.ipAddressNumAVG,c.ipAddressNumSTD
,d.ipDeviceNumAVG,d.ipDeviceNumSTD,e.deviceNameNumAVG,e.deviceNameNumSTD,f.devicePhoneNumAVG,f.devicePhoneNumSTD
,g.deviceAddressNumAVG,g.deviceAddressNumSTD,h.phoneAddressNumAVG,h.phoneAddressNumSTD,i.phoneNameNumAVG,i.phoneNameNumSTD,j.nameAddressNumAVG,j.nameAddressNumSTD
from (SELECT avg(ipPhoneNum) ipPhoneNumAVG,stddev_samp(ipPhoneNum) ipPhoneNumSTD FROM (SELECT  ipPhone,ipPhoneNum FROM algorithm_data.HnMember_info_c GROUP BY ipPhone,ipPhoneNum) a ) a
join (SELECT avg(ipNameNum) ipNameNumAVG,stddev_samp(ipNameNum) ipNameNumSTD FROM (SELECT  ipName,ipNameNum FROM algorithm_data.HnMember_info_c GROUP BY ipName,ipNameNum) b ) b
join (SELECT avg(ipAddressNum) ipAddressNumAVG,stddev_samp(ipAddressNum) ipAddressNumSTD FROM (SELECT  ipAddress,ipAddressNum FROM algorithm_data.HnMember_info_c GROUP BY ipAddress,ipAddressNum) c ) c
join (SELECT avg(ipDeviceNum) ipDeviceNumAVG,stddev_samp(ipDeviceNum) ipDeviceNumSTD FROM (SELECT  ipDevice,ipDeviceNum FROM algorithm_data.HnMember_info_c GROUP BY ipDevice,ipDeviceNum) d ) d
join (SELECT avg(deviceNameNum) deviceNameNumAVG,stddev_samp(deviceNameNum) deviceNameNumSTD FROM (SELECT  deviceName,deviceNameNum FROM algorithm_data.HnMember_info_c GROUP BY deviceName,deviceNameNum) e ) e
join (SELECT avg(devicePhoneNum) devicePhoneNumAVG,stddev_samp(devicePhoneNum) devicePhoneNumSTD FROM (SELECT  devicePhone,devicePhoneNum FROM algorithm_data.HnMember_info_c GROUP BY devicePhone,devicePhoneNum) f ) f
join (SELECT avg(deviceAddressNum) deviceAddressNumAVG,stddev_samp(deviceAddressNum) deviceAddressNumSTD FROM (SELECT  deviceAddress,deviceAddressNum FROM algorithm_data.HnMember_info_c GROUP BY deviceAddress,deviceAddressNum) g ) g
join (SELECT avg(phoneAddressNum) phoneAddressNumAVG,stddev_samp(phoneAddressNum) phoneAddressNumSTD FROM (SELECT  phoneAddress,phoneAddressNum FROM algorithm_data.HnMember_info_c GROUP BY phoneAddress,phoneAddressNum) h) h
join (SELECT avg(phoneNameNum) phoneNameNumAVG,stddev_samp(phoneNameNum) phoneNameNumSTD FROM (SELECT  phoneName,phoneNameNum FROM algorithm_data.HnMember_info_c GROUP BY phoneName,phoneNameNum) i ) i
join (SELECT avg(nameAddressNum) nameAddressNumAVG,stddev_samp(nameAddressNum) nameAddressNumSTD FROM (SELECT  nameAddress,nameAddressNum FROM algorithm_data.HnMember_info_c GROUP BY nameAddress,nameAddressNum) j ) j
limit 1
;

use algorithm_data;
drop table if exists HnMember_info;
create table HnMember_info AS
select b.*
,(ipPhoneNum-ipPhoneNumAVG)/ipPhoneNumSTD ipPhoneNumZSCORE
,(ipNameNum-ipNameNumAVG)/ipNameNumSTD ipNameNumZSCORE
,(ipAddressNum-ipAddressNumAVG )/ipAddressNumSTD ipAddressNumZSCORE
,(ipDeviceNum-ipDeviceNumAVG)/ipDeviceNumSTD ipDeviceNumZSCORE
,(deviceNameNum-deviceNameNumAVG)/deviceNameNumSTD deviceNameNumZSCORE
,(devicePhoneNum-devicePhoneNumAVG )/devicePhoneNumSTD devicePhoneNumZSCORE
,(deviceAddressNum-deviceAddressNumAVG)/deviceAddressNumSTD deviceAddressNumZSCORE
,(phoneAddressNum-phoneAddressNumAVG )/phoneAddressNumSTD phoneAddressNumZSCORE
,(phoneNameNum-phoneNameNumAVG)/phoneNameNumSTD phoneNameNumZSCORE
,(nameAddressNum-nameAddressNumAVG )/nameAddressNumSTD nameAddressNumZSCORE
from HnMember_info_d a
join HnMember_info_c b on 1=1
;



-------paper need value----
--(select * from HnMember_info_b where to_date(online_last_sale_time) >='2018-03-13' )
--(select * from HnMember_info_b where year(register_time) in (2018) and month(register_time) in (3))
use algorithm_data;
drop table if exists HnMember_info;
create table HnMember_info AS
select a.*
,b.ipPhoneNum,c.ipNameNum,d.ipAddressNum,f.deviceNameNum,g.devicePhoneNum,h.deviceAddressNum
,i.devmemnum,j.phonememnum,k.sameaddressmemnum,l.ipmemnum,m.namememnum
from (select * from HnMember_info_b where to_date(online_last_sale_time) >='2018-03-13'  ) a
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

select count(distinct member_id) null_num,count(distinct deviceName)  num
from algorithm_data.HnMember_info
where device_id!='999999' and deviceNameNum>2
;

select a.null_num,b.value_num
from (select count(distinct member_id) null_num from algorithm_data.HnMember_info where recept_phone='999999') a
join (select count(distinct member_id) value_num from algorithm_data.HnMember_info where recept_phone!='999999') b
limit 1
;

select count(distinct a.recept_address_same),count(distinct b.member_id) value_num
from (select recept_address_same,count(distinct member_id) value_num from algorithm_data.HnMember_info where recept_address_same!='999999' group by recept_address_same) a
join algorithm_data.HnMember_info b on a.recept_address_same=b.recept_address_same
where a.value_num>2
;

select count(distinct ip),count(distinct device_id),count(distinct recept_address_same),count(distinct recept_name),count(distinct recept_phone)
from algorithm_data.HnMember_info
;

use algorithm_data;
drop table if exists test1;
create table test1 as
select a.circle_id,a.circle_size,a.order_num,b.*
from (select *  from algorithm_data.hnCircleScore where category='20180322nonRealTime' and year(register_time)=2018 and month(register_time)=3) a
join algorithm_data.HnMember_info b on a.circle_memberid=b.member_id and a.device_id=b.device_id and a.recept_name=b.recept_name and a.ip=b.ip
;

use algorithm_data;
drop table if exists test2;
create table test2 as
select b.*
from (select  circle_id,count(distinct member_id) AS circle_num from algorithm_data.test1 group by circle_id) a
join  algorithm_data.test1 b on a.circle_id=b.circle_id
where a.circle_num>8
;



cd /home/bigdatadev/fuzheng
hive -e"select min(Arrivaldate) as minArrivaldate from dw_htlbizdb.fztmp_OrderTimeRoom;" >ab.txt

-----------------------------



-----实时算法

use algorithm_data;
drop table if exists test_hnTimeIpDeviceA;
create table test_hnTimeIpDeviceA AS
select a.member_id,b.user_ip as ip ,c.device_no as device_id,a.sale_time
from  (select member_id,parent_order_no,order_no,sale_time   from  mdata.c_order_snap  where online_order_ind='1' and year(sale_time)>=2018 ) a
left join (select  order_parent_no,user_ip from sourcedata.s09_oms_order_parent where  user_ip is not null or trim(user_ip)!='') b on a.parent_order_no=b.order_parent_no
left join (select  order_no,device_no  from mdata.c_order_detail where  device_no is not null or trim(device_no)!='')  c on a.order_no=c.order_no
group by a.member_id,b.user_ip ,c.device_no,a.sale_time
;
