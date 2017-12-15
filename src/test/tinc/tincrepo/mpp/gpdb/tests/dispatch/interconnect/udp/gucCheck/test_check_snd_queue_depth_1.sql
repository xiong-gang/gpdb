--
-- @description Interconncet flow control test case: verify the change of guc value take affect
-- @created 2012-11-06
-- @modified 2012-11-06
-- @tags executor
-- @gpdb_version [4.2.3.0,main]

-- Set GUC
SET gp_interconnect_fc_method = "capacity";

-- Create a table
CREATE TABLE small_table(dkey INT, jkey INT, rval REAL, tval TEXT default 'abcdefghijklmnopqrstuvwxyz') DISTRIBUTED BY (dkey);
CREATE TABLE small_table1(dkey INT, jkey INT, rval REAL, tval TEXT default 'abcdefghijklmnopqrstuvwxyz') DISTRIBUTED BY (dkey);

-- Generate some data
INSERT INTO small_table VALUES(generate_series(1, 50000), generate_series(50001, 100000), sqrt(generate_series(50001, 100000)));
INSERT INTO small_table1 VALUES(generate_series(1, 50000), generate_series(50001, 100000), sqrt(generate_series(50001, 100000)));

-- Functional tests
select count(*) from small_table t1, small_table1 t2 where t1.jkey = t2.jkey;

-- drop table testemp
DROP TABLE small_table;
DROP TABLE small_table1;
