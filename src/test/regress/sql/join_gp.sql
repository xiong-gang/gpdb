
-- test numeric hash join

set enable_hashjoin to on;
set enable_mergejoin to off;
set enable_nestloop to off;
create table nhtest (i numeric(10, 2)) distributed by (i);
insert into nhtest values(100000.22);
insert into nhtest values(300000.19);
explain select * from nhtest a join nhtest b using (i);
select * from nhtest a join nhtest b using (i);

create temp table l(a int);
insert into l values (1), (1), (2);
select * from l l1 join l l2 on l1.a = l2.a left join l l3 on l1.a = l3.a and l1.a = 2 order by 1,2,3;

--
-- test hash join
--

create table hjtest (i int, j int) distributed by (i,j);
insert into hjtest values(3, 4);

select count(*) from hjtest a1, hjtest a2 where a2.i = least (a1.i,4) and a2.j = 4;

--
-- Predicate propagation over equality conditions
--

drop schema if exists pred;
create schema pred;
set search_path=pred;

create table t1 (x int, y int, z int) distributed by (y);
create table t2 (x int, y int, z int) distributed by (x);
insert into t1 select i, i, i from generate_series(1,100) i;
insert into t2 select * from t1;

analyze t1;
analyze t2;

--
-- infer over equalities
--
explain select count(*) from t1,t2 where t1.x = 100 and t1.x = t2.x;
select count(*) from t1,t2 where t1.x = 100 and t1.x = t2.x;

--
-- infer over >=
--
explain select * from t1,t2 where t1.x = 100 and t2.x >= t1.x;
select * from t1,t2 where t1.x = 100 and t2.x >= t1.x;

--
-- multiple inferences
--

set optimizer_segments=2;
explain select * from t1,t2 where t1.x = 100 and t1.x = t2.y and t1.x <= t2.x;
reset optimizer_segments;
select * from t1,t2 where t1.x = 100 and t1.x = t2.y and t1.x <= t2.x;

--
-- MPP-18537: hash clause references a constant in outer child target list
--

create table hjn_test (i int, j int) distributed by (i,j);
insert into hjn_test values(3, 4);
create table int4_tbl (f1 int);
insert into int4_tbl values(123456), (-2147483647), (0), (-123456), (2147483647);
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,4) and hjn_test.j = 4;
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,(array[4])[1]) and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where least (foo.bar,(array[4])[1]) = hjn_test.i and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar, least(4,10)) and hjn_test.j = least(4,10);
select * from int4_tbl a join int4_tbl b on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1));

set enable_hashjoin to off;
-- Disable hashjoin forms fo ORCA
select disable_xform('CXformInnerJoin2HashJoin');
select disable_xform('CXformLeftAntiSemiJoin2HashJoin');
select disable_xform('CXformLeftAntiSemiJoinNotIn2HashJoinNotIn');
select disable_xform('CXformLeftOuterJoin2HashJoin');
select disable_xform('CXformLeftSemiJoin2HashJoin');

select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,4) and hjn_test.j = 4;
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,(array[4])[1]) and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where least (foo.bar,(array[4])[1]) = hjn_test.i and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar, least(4,10)) and hjn_test.j = least(4,10);
select * from int4_tbl a join int4_tbl b on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1));

reset enable_hashjoin;

-- Enable hashjoin forms fo ORCA
select enable_xform('CXformInnerJoin2HashJoin');
select enable_xform('CXformLeftAntiSemiJoin2HashJoin');
select enable_xform('CXformLeftAntiSemiJoinNotIn2HashJoinNotIn');
select enable_xform('CXformLeftOuterJoin2HashJoin');
select enable_xform('CXformLeftSemiJoin2HashJoin');

drop schema pred cascade;
