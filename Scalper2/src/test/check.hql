--初始筛选剩下会员个数
select count(distinct member_id)
from algorithm_data.HnAllOrder_aa where order_no in
(

);

--进入模型会员个数
select count(distinct member_id)
from algorithm_data.HnAllOrder_spark
where member_id in ()
;


--最终识别黄牛会员详情
select *
from algorithm_data.HnDoubtfulOrder_all
where recept_phone in ()
;

select *
from algorithm_data.HnDoubtfulOrder_all
where circle_id in ('100000000230224','100000002127578')
;


select *
from algorithm_data.hnallorder_circleResult
where circle_id in ('100000000230224','100000002127578')
;
