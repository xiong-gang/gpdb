-- Test concurrent update a table with a varying length type
CREATE TABLE t_concurrent_update(a int, b int, c char(84));
INSERT INTO t_concurrent_update VALUES(1,1,'test');

1: BEGIN;
1: SET optimizer=off;
1: UPDATE t_concurrent_update SET b=b+10 WHERE a=1;
2: SET optimizer=off;
2&: UPDATE t_concurrent_update SET b=b+10 WHERE a=1;
1: END;
2<:
1: SELECT * FROM t_concurrent_update;
1q:
2q:

DROP TABLE t_concurrent_update;


-- Test the concurrent update transaction order on the segment is reflected on master
CREATE TABLE t_concurrent_update(a int, b int);
INSERT INTO t_concurrent_update VALUES(1,1);

1: BEGIN;
1: UPDATE t_concurrent_update SET b=b+10 WHERE a=1;
2: BEGIN;
-- transaction 2 will wait transaction 1 on the segment
2&: UPDATE t_concurrent_update SET b=b+10 WHERE a=1;
-- transaction 1 suspend before commit, but it will wake up transaction 2 on segment
1: select gp_inject_fault('before_xact_end_procarray', 'suspend', '', '', '', 1, 1, 0, 1);
1&: END;
-- transaction 2 should wait transaction 1 commit on master
2&: END;
select gp_inject_fault('before_xact_end_procarray', 'reset', 1);
-- the query should not get the incorrect distributed snapshot: transaction 1 in-progress
-- and transaction 2 finished
SELECT * FROM t_concurrent_update;
1<:
2<:
1q:
2q:

DROP TABLE t_concurrent_update;
