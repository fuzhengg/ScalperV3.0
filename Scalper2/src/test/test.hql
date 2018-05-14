--1. 不同属性值对应的人群
use algorithm_data;
drop table if exists hn_test_a;
create table hn_test_a AS
select distinct ip,member_id
from algorithm_data.HnAllOrder_spark
;
use algorithm_data;
drop table if exists hn_test_a1;
create table hn_test_a1 AS
select a.*
from (select ip,count(distinct member_id) ip_member_num
from algorithm_data.hn_test_a
group by ip ) a
where ip_member_num>1 and ip is not NULL and ip <>'999999'
;
use algorithm_data;
drop table if exists hn_test_a2;
create table hn_test_a2 AS
select distinct a.ip,b.member_id
from algorithm_data.hn_test_a1 a
join algorithm_data.hn_test_a b on a.ip=b.ip
;

use algorithm_data;
drop table if exists hn_test_a3;
create table hn_test_a3 AS
select a.ip,concat_ws(',', collect_set(String(a.member_id))) as member_array
from algorithm_data.hn_test_a2 a
group by a.ip
;


--device_id
use algorithm_data;
drop table if exists hn_test_b;
create table hn_test_b AS
select distinct device_id,member_id
from algorithm_data.HnAllOrder_spark
;
use algorithm_data;
drop table if exists hn_test_b1;
create table hn_test_b1 AS
select a.*
from (select device_id,count(distinct member_id) device_id_member_num
from algorithm_data.hn_test_b
group by device_id ) a
where device_id_member_num>1 and device_id is not NULL and device_id <>'999999'
;
use algorithm_data;
drop table if exists hn_test_b2;
create table hn_test_b2 AS
select distinct a.device_id,b.member_id
from algorithm_data.hn_test_b1 a
join algorithm_data.hn_test_b b on a.device_id=b.device_id
;

use algorithm_data;
drop table if exists hn_test_b3;
create table hn_test_b3 AS
select a.device_id,concat_ws(',', collect_set(String(a.member_id))) as member_array
from algorithm_data.hn_test_b2 a
group by a.device_id
;
