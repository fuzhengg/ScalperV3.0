#!/usr/bin/env bash
###################################       Environment
#work_dir=$1
#iniYear=$2
#iniMonth=$3
#iniDay=$4
##################
work_dir='/home/hive/fuzheng/Scalper3/online-2017-11-24/'
iniYear=`date +%Y`
iniMonth=`date +%m`
iniDay=999
yesterday="`date  -d -1day +%Y-%m-%d`"
cd $work_dir
echo '1.开始检查注册月份为:'${iniYear} '年' ${iniMonth} '月的会员'

#检查最终落表情况所用到的参数
iterations=0
dataOK=100
url='http://10.201.32.182:7060/dp-compassServer/api/v1/order/redis/aorder'


#################
###############################             mail Environment
#RECIPIENT="['Zheng.Fu@bl.com']"
subject='ScalperResult'
main_mail="${work_dir}mail.txt"
ATTACHMENTS="['${work_dir}result.xlsx','${work_dir}HnOrder.xlsx','${work_dir}run.log']"
RECIPIENT_ERROR="['Zheng.Fu@bl.com','JiangFeng.Guo@bl.com']"
subject_ERROR='ScalperCheckResult---ERROR---ERROR---RERROR!!!'
RECIPIENT="['JunKe.Zhao@bl.com','Zheng.Fu@bl.com','JiangFeng.Guo@bl.com','Jie.Xiong@bl.com']"
#RECIPIENT_ERROR="['JunKe.Zhao@bl.com','JianLu.Xuan@bl.com','Zheng.Fu@bl.com','v-ShanShan.Zhang@bl.com']"
main_mail_ERROR="${work_dir}run.log"
ATTACHMENTS_ERROR="['${work_dir}run.log']"


#####################################      1.get iniData
echo "2. 清洗数据"
echo "hive -hiveconf Year=${iniYear} -hiveconf Month=${iniMonth} -f iniData.hql > ${work_dir}run.log"
hive -hiveconf Year=${iniYear} -hiveconf Month=${iniMonth} -f iniData.hql  > ${work_dir}sql.log 2>&1


####################################       2. run Algorithm
echo "3. 运行模型："
sparkinfo="/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit   --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 30g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096 --deploy-mode cluster  ${work_dir}Scalper.jar  $iniYear  $iniMonth $iniDay"
echo $sparkinfo
/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit   --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 30g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096 --deploy-mode cluster  ${work_dir}Scalper.jar  $iniYear  $iniMonth $iniDay  >> ${work_dir}sql.log 2>&1
#/opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit   --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 30g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096 --deploy-mode cluster  Scalper.jar  2017 10 999


###################################        3.check Result
#将结果写入指定的表里，等待调用接口
WritResult="
use algorithm_data;
drop table if exists algorithm_data.HnDoubtfulOrder_all;
create table algorithm_data.HnDoubtfulOrder_all AS
SELECT distinct String(circle_memberid) as  member_id,recept_phone, recept_name, recept_address_detail,ip,device_id,register_time
,circle_id,circle_size, '严重怀疑' as level
FROM algorithm_data.hnallorder_circleResult
where year<=$iniYear and month<=$iniMonth
;

use algorithm_data;
drop table if exists HnAllOrder_test1;
create table HnAllOrder_test1 as
SELECT distinct
a.member_id,c.recept_name,c.recept_phone
,c.recept_address_detail
,b.goods_name,b.goods_code,b.item_amount
,b.sale_price*b.sale_sum-b.item_amount as detailorder_discount
,b.sale_sum as goods_salesum,b.order_detail_no
,b.sale_money_sum,a.discount_money_sum,a.need_money,a.sale_time
,a.order_source
,a.order_type, COALESCE( a.order_no,b.order_no) order_no
FROM mdata.c_order_snap a
join mdata.c_order_detail b on a.order_no=b.order_no
join  sourcedata.s03_oms_order_sub c on a.order_no=c.order_no
where to_date(a.sale_time)='"$yesterday"'
;
"

#当天计算得到的当月黄牛及购物详情，作为邮件附件
getCheckResult="
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select distinct circle_memberid as member_id,ip, device_id,recept_address_detail,recept_name,recept_phone,register_time, circle_id, circle_size
from algorithm_data.hnallorder_circleResult
where year=$iniYear and month=$iniMonth and  day=$iniDay;
"

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
from (select year, month,count(DISTINCT circle_memberid) HnMemberNum
,count(distinct circle_id) circle_num,max(circle_size) MaxCircle_size
from algorithm_data.hnallorder_circleResult group by year, month ) a
join algorithm_data.HnSaledMemberNum b on a.year=b.year and a.month=b.month
order by a.year, a.month
;"
hive -e "$WritResult"  >> ${work_dir}sql.log 2>&1
QA=`hadoop fs -du -s -h  hdfs://nameservice1/user/hive/warehouse/algorithm_data.db/hndoubtfulorder_all`
resultQA=`echo   $QA   |   awk  '{print   $1} '`
isOK=$(echo "$resultQA > $dataOK" | bc)
echo '3. 最终数据集大小为:'${resultQA}'KB'



while (( $iterations < 10 ))
do
        if [ $isOK -eq 1 ] && (( $iterations < 10 ))
        then
                echo "4. 黄牛全量执行完成，开始调用接口"
                APIresult=`curl $url`
                echo "调用接口返回结果："$APIresult
                ApiIsOk=`echo $APIresult  |awk  -F ':'  '{print $2}' |awk  -F ','  '{print $1}'`
                echo "调用接口是否成功？"${ApiIsOk}
                if [ "$ApiIsOk" == '"success"' ];then
                    echo "5. 接口调用完毕，开始发送邮件"
                    hive -e "$getCheckResult" > ${work_dir}result.txt
                    hive -e "$getHnOrder" > ${work_dir}HnOrder.txt
                    echo "2017.11月及之前月份：当月注册，并截止2017.11月有消费记录的会员，对应的黄牛筛选情况" > ${work_dir}mail.txt
                    echo "2017.12月及之后月份：当月消费，并在历史黄牛记录中无记录的会员，对应的黄牛筛选情况"   >> ${work_dir}mail.txt
                    hive -e "$getStaticsResult" >> ${work_dir}mail.txt
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/txt2xlsx.py ${work_dir}result.txt ${work_dir}result.xlsx
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/txt2xlsx.py ${work_dir}HnOrder.txt ${work_dir}HnOrder.xlsx
                    /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/mail.py $RECIPIENT $subject $main_mail $ATTACHMENTS $work_dir
                    break
                else
                    echo "5. 黄牛全量调用接口出错，继续循环！"
                    continue
                fi
        elif (( $iterations >= 10 ))
        then
            echo "4. 黄牛全量执行出错，循环无效，发邮件警告！"
            /opt/anaconda2/bin/python2.7 /home/hive/fuzheng/Mail/mail.py $RECIPIENT_ERROR $subject_ERROR $main_mail_ERROR $ATTACHMENTS_ERROR $work_dir
            break
        fi
        echo "黄牛全量执行出错，开始循环！"
        echo "黄牛全量执行出错，开始循环第$iterations 次！"
        echo "Spark重新运行:"
        echo  $sparkinfo
        /opt/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit   --executor-cores 8 --num-executors 18 --executor-memory 20g --master yarn  --driver-memory 30g --conf spark.kryoserializer.buffer.max=656m  --conf spark.kryoserializer.buffer=64m   --conf spark.driver.maxResultSize=2g --conf spark.shuffle.manager=SORT --conf spark.yarn.executor.memoryOverhead=4096 --deploy-mode cluster  ${work_dir}Scalper.jar  $iniYear  $iniMonth $iniDay  >> ${work_dir}sql.log 2>&1
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
