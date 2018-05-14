#!/usr/bin/env bash
###################################       Environment
##目录
work_dir='/home/hive/fuzheng/Scalper3/online-2018-03-22/'
cd $work_dir

##参数
yesterday="`date  -d -1day +%Y-%m-%d`"
StartDate="`date  -d "30 day ago $yesterday" +%Y-%m-%d`"
iniYear="`date  -d "0 day ago $yesterday" +%Y`"
iniMonth="`date  -d "0 day ago $yesterday" +%m`"
iniDay="`date  -d "0 day ago $yesterday" +%d`"
#检查最终落表情况所用到的参数
iterations=0
dataOK=100
url='http://10.201.32.182:7060/dp-compassServer/api/v1/order/redis/aorder'
#邮件参数
subject='ScalperResult'
main_mail="${work_dir}mail.txt"
ATTACHMENTS="['${work_dir}result.xlsx','${work_dir}HnOrder.xlsx','${work_dir}run.log']"
RECIPIENT_ERROR="['Zheng.Fu@bl.com']"
subject_ERROR='ScalperCheckResult---ERROR---ERROR---RERROR!!!'
RECIPIENT="['Zheng.Fu@bl.com']"
#RECIPIENT="['JunKe.Zhao@bl.com','Zheng.Fu@bl.com','JiangFeng.Guo@bl.com','Jie.Xiong@bl.com']"
#RECIPIENT_ERROR="['JunKe.Zhao@bl.com','JianLu.Xuan@bl.com','Zheng.Fu@bl.com','v-ShanShan.Zhang@bl.com']"
main_mail_ERROR="${work_dir}run.log"
ATTACHMENTS_ERROR="['${work_dir}run.log']"




echo '1.开始检查注册月份为:'$yesterday'日''之前30天的会员'
echo "2. 清洗数据"
echo "hive -hiveconf StartDate=${StartDate}  -f iniData.hql  > ${work_dir}sql.log 2>&1"
#hive -hiveconf StartDate=${StartDate}  -f iniData.hql  > ${work_dir}sql.log 2>&1


echo "3. 运行模型："
sparkinfo="/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit  --executor-cores 6 --num-executors 19 --executor-memory 5g --master yarn  --driver-memory 40g  ${work_dir}Scalper.jar   $iniYear  $iniMonth $iniDay     >> ${work_dir}sql.log 2>&1"
echo $sparkinfo
/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit  --executor-cores 7 --num-executors 20 --executor-memory 20g --master yarn  --driver-memory 20g  ${work_dir}Scalper.jar   $iniYear  $iniMonth $iniDay     >> ${work_dir}sql.log 2>&1
##/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit  --executor-cores 7 --num-executors 20 --executor-memory 30g --master yarn  --driver-memory 40g  ${work_dir}Scalper.jar   2018  04 11   >> ${work_dir}sql.log 2>&1
##/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-shell --executor-cores 7 --num-executors 5 --executor-memory 5g --master yarn  --driver-memory 2g --name Scalper_test


echo "4.check Result"
#将结果写入指定的表里，等待调用接口
#最终黄牛结果: 去除号码白名单中涉及到的所有号码，去除会员白名单中涉及到的所有会员,两者交集是最终的黄牛结果
WritResult="
use algorithm_data;
drop table if exists HnAllOrder_test2;
create table HnAllOrder_test2 as
select circle_id, circle_size, score, String(circle_memberid) as  member_id, ip, device_id, recept_address_detail
, b.recept_name, recept_phone, a.register_time, registertime_score,deviceidscore,saletime_score,needmoney_score,discountmoney_score
,case when int(ecp_ind)=0  then  '严重怀疑' else '非常怀疑' end as level
FROM (select * from algorithm_data.hnCircleScore where category='20180322nonRealTime') a
left join mdata.c_order_snap  b on a.circle_memberid=b.member_id
left join mdata.c_member_info_snap  c on a.circle_memberid=c.member_id
;

--去除白名单和ECP会员ID
use algorithm_data;
drop table if exists algorithm_data.test1;
create table algorithm_data.test1 AS
select b.member_id,b.recept_phone, b.recept_name, b.recept_address_detail,b.ip,b.device_id,b.register_time
,b.circle_id,b.circle_size, b.level
from algorithm_data.Hn_PhonewhiteList a
right join algorithm_data.HnAllOrder_test2 b on a.whitephone=b.recept_phone and b.level='严重怀疑'
where a.whitephone is NULL and b.recept_phone is not NULL and b.level='严重怀疑'
;

use algorithm_data;
drop table if exists algorithm_data.test2;
create table algorithm_data.test2 AS
select b.member_id,b.recept_phone, b.recept_name, b.recept_address_detail,b.ip,b.device_id,b.register_time
,b.circle_id,b.circle_size, b.level
from algorithm_data.Hn_MemberwhiteList a
right join algorithm_data.test1 b on a.whitemember=b.member_id  and b.level='严重怀疑'
where a.whitemember is NULL and b.member_id is not NULL and b.level='严重怀疑'
;



use algorithm_data;
drop table if exists algorithm_data.test3;
create table algorithm_data.test3 AS
select a.*
 from  algorithm_data.test2 a
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') b on a.recept_phone=b.recept_phone
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') c on a.member_id=c.member_id
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') d on a.recept_address_detail=d.recept_address_detail
 left join ( select *   from algorithm_data.HnDoubtfulOrder_all_back  where special='black') e on a.device_id=e.device_id
 where (b.recept_phone is null or b.recept_phone in ('')) and  (c.member_id in ('') or c.member_id is null)
and  (d.recept_address_detail in ('') or d.recept_address_detail is null)   and  (e.device_id in ('') or e.device_id is null)
 ;


insert  overwrite table algorithm_data.HnDoubtfulOrder_all_back partition (day='$yesterday' ,special='normal')
select member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,circle_id,circle_size, level
from algorithm_data.test3
group by member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,circle_id,circle_size, level
;


use algorithm_data;
drop table if exists algorithm_data.HnDoubtfulOrder_all;
create table algorithm_data.HnDoubtfulOrder_all AS
select member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,string(circle_id) circle_id,string(circle_size) circle_size, level
from algorithm_data.test3
group by member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time,string(circle_id),string(circle_size), level,day,special
;





use algorithm_data;
drop table if exists HnAllOrder_test1;
create table HnAllOrder_test1 as
SELECT distinct
member_id,recept_name,recept_phone
,recept_address_detail
,b.goods_name,b.goods_code,b.item_amount
,b.sale_price*b.sale_sum-b.item_amount as detailorder_discount
,b.sale_sum as goods_salesum,order_detail_no
,a.sale_money_sum,a.discount_money_sum,a.need_money,a.sale_time
,order_source
,order_type, COALESCE( a.order_no,b.order_no) order_no
from algorithm_data.HnOrder_info a
join mdata.c_order_detail b on a.order_no=b.order_no
where to_date(a.sale_time)='"$yesterday"'
;
"

#当天计算得到的黄牛,作为邮件附件
getCheckResult="
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select distinct circle_memberid as member_id,a.ip, a.device_id,recept_address_detail,a.recept_name,recept_phone,a.register_time, circle_id, circle_size
from (select * from algorithm_data.hnCircleScore where year=$iniYear and month=$iniMonth and  day=$iniDay  and category='20180322nonRealTime'  ) a
join algorithm_data.HnMember_info b on a.circle_memberid=b.member_id and a.ip=b.ip and a.device_id=b.device_id and a.recept_name=b.recept_name and a.register_time=b.register_time  and int(b.ecp_ind)=0
;
"
#已经拦截黄牛购物详情，作为邮件附件
getHnOrder="
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
SELECT DISTINCT c.circle_id,c.circle_size
,a.member_id,c.device_id,a.recept_name,a.recept_phone
,a.recept_address_detail
,a.goods_name,a.goods_code,a.item_amount,a.detailorder_discount,a.goods_salesum,a.order_detail_no
,a.sale_money_sum,a.discount_money_sum,a.need_money,a.sale_time
,a.order_source
,a.order_type, COALESCE( a.order_no,b.order_no) order_no
,b.abn_log_record,b.follow_up_action
FROM (SELECT * FROM  sourcedata.s03_OMS_ORDER_ABN_INFO WHERE abn_code = '123' AND to_date(create_time)='"$yesterday"') b
LEFT JOIN algorithm_data.HnAllOrder_test1 a ON a.order_no=b.order_no
LEFT JOIN algorithm_data.HnDoubtfulOrder_all c ON a.member_id=c.member_id
;"


#统计黄牛数量，作为邮件正文
getStaticsResult="
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select a.year, a.month,a.HnMemberNum,b.MemberNum,FLOAT(a.HnMemberNum/b.MemberNum) as HnPercent,a.circle_num,a.MaxCircle_size
from (select year, case when year<=2017 then 99 else month end month,count(DISTINCT circle_memberid) HnMemberNum
      ,count(distinct circle_id) circle_num,max(circle_size) MaxCircle_size
     from algorithm_data.hnCircleScore  where category='20180322nonRealTime'  group by year, case when year<=2017 then 99 else month end ) a
join algorithm_data.HnSaledMemberNum b on a.year=b.year and a.month=b.month
order by a.year, a.month
;"
hive -e "$WritResult"  >> ${work_dir}sql.log 2>&1
QA=`hadoop fs -du -s -h  hdfs://nameservice1/user/hive/warehouse/algorithm_data.db/hndoubtfulorder_all`
resultQA=`echo   $QA   |   awk  '{print   $1} '`
isOK=$(echo "$resultQA > $dataOK" | bc)
echo '5. 最终数据集大小为:'${resultQA}'KB'



while (( $iterations < 10 ))
do
        if [ $isOK -eq 1 ] && (( $iterations < 10 ))
        then
                echo "6. 黄牛全量执行完成，开始调用接口"
                APIresult=`curl $url`
                echo "调用接口返回结果："$APIresult
                ApiIsOk=`echo $APIresult  |awk  -F ':'  '{print $2}' |awk  -F ','  '{print $1}'`
                echo "7. 调用接口是否成功？"${ApiIsOk}
                if [ "$ApiIsOk" == '"success"' ];then
                    echo "8. 接口调用完毕，开始发送邮件"
                    hive -e "$getCheckResult" > ${work_dir}result.txt
                    hive -e "$getHnOrder" > ${work_dir}HnOrder.txt
                    hive -e "$getStaticsResult" > ${work_dir}mail.txt
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/txt2xlsx.py ${work_dir}result.txt ${work_dir}result.xlsx
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/txt2xlsx.py ${work_dir}HnOrder.txt ${work_dir}HnOrder.xlsx
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/mail.py $RECIPIENT $subject $main_mail $ATTACHMENTS $work_dir
                    break
                else
                    echo "8. 黄牛全量调用接口出错，继续循环！"
                    continue
                fi
        elif (( $iterations >= 10 ))
        then
            echo "6. 黄牛全量执行出错，循环无效，发邮件警告！"
            /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/mail.py $RECIPIENT_ERROR $subject_ERROR $main_mail_ERROR $ATTACHMENTS_ERROR $work_dir
            break
        fi
        echo "黄牛全量执行出错，开始循环！"
        echo "黄牛全量执行出错，开始循环第$iterations 次！"
        echo "Spark重新运行:"
        echo  $sparkinfo
        /opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit  --executor-cores 6 --num-executors 19 --executor-memory 5g --master yarn  --driver-memory 40g  ${work_dir}Scalper.jar   $iniYear  $iniMonth $iniDay     >> ${work_dir}sql.log 2>&1
        #写入指定表格
        hive -e "$WritResult"  >> ${work_dir}sql.log 2>&1
        QA=`hadoop fs -du -s -h  hdfs://nameservice1/user/hive/warehouse/algorithm_data.db/hndoubtfulorder_all`
        resultQA=`echo   $QA   |   awk  '{print   $1} '`
        isOK=$(echo "$resultQA > $dataOK" | bc)
        echo '数据集大小为:'${resultQA}'KB'
        iterations=$(($iterations+1))
        sleep 60
done

























#
#
#
#
#
#
#
#
#
#CREATE TABLE algorithm_data.allConsumMember (
#    xmember_id bigint
#  , ymember_id bigint
#)
#PARTITIONED BY (year int,month int)
#;
#
#
#CREATE TABLE algorithm_data.hnallorder_circleResult (
#   circle_id bigint
#  ,circle_size  bigint
#  ,circle_memberID bigint
#  ,ip	string
#  ,device_id	string
#  ,recept_address_same	string
#  ,recept_address_detail	string
#  ,recept_name	string
#  ,recept_phone	string
#  ,register_phone	string
#  ,recept_mobilearea_a	string
#  ,register_mobilearea_a	string
#  ,register_time	string
#  ,online_first_sale_time	string
#  ,online_last_sale_time	string
#  ,online_first_need_amt	float
#  ,online_last_need_amt	float
#  ,online_total_need_amt	float
#  ,first_acquire_time	string
#  ,last_acquire_time	string
#  ,getcouponsecdif	float
#  ,orderdetailgoodscost	float
#  ,avg_detailorder_discount	float
#  ,coupon_amount	float
#  ,avgcouponuse	float
#  ,getcouponnum	float
#)
#PARTITIONED BY (year int,month int,day int)
#;
#
#
#
#
#
#
#
#use algorithm_data;
#drop table if exists HnAllOrder_spark_LG;
#create table HnAllOrder_spark_LG AS
#select distinct
#BIGINT(a.member_id) member_id
#,BIGINT(a.member_level_id) member_level_id
#,BIGINT(a.ECP_ind) ECP_ind
#,a.register_time
#,unix_timestamp(a.online_last_sale_time) online_last_sale_time
#,unix_timestamp(a.online_first_sale_time) online_first_sale_time
#,Double(a.online_first_sale_amt) online_first_sale_amt
#,Double(a.online_first_discount_amt) online_first_discount_amt
#,Double(a.online_last_sale_amt) online_last_sale_amt
#,Double(a.online_last_discount_amt) online_last_discount_amt
#,Double(a.online_total_sale_amt) online_total_sale_amt
#,Double(a.online_total_discount_amt) online_total_discount_amt
#,Double(b.getCouponSecDifAvg) getCouponSecDifAvg
#,Double(b.getCouponNum) getCouponNum
#from algorithm_data.HnAllOrder_ab a
#join
#(select member_id
#,avg(  unix_timestamp(acquire_time)-unix_timestamp(create_time) )as getCouponSecDifAvg
#,count(distinct coupon_code) getCouponNum
#from sourcedata.s01_camp_user_coupon
#GROUP BY member_id) b on a.member_id=b.member_id
#;
