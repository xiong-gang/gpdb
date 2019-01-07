-- This test assumes 3 primaries and 3 mirrors from a gpdemo segwalrep cluster

create extension if not exists gp_inject_fault;

include: helpers/server_helpers.sql;

-- make sure we are in-sync for the primary we will be testing with
select content, role, preferred_role, mode, status from gp_segment_configuration;

-- print synchronous_standby_names should be set to '*' at start of test
0U: show synchronous_standby_names;

-- create table and show commits are not blocked
create table segwalrep_commit_blocking (a int) distributed by (a);
insert into segwalrep_commit_blocking values (1);

-- skip FTS probes always
select gp_inject_fault('fts_probe', 'reset', dbid)
    from gp_segment_configuration
    where content = -1 and role = 'p';
select gp_inject_fault_infinite('fts_probe', 'skip', dbid)
    from gp_segment_configuration
    where content = -1 and role = 'p';
-- force scan to trigger the fault
select gp_request_fts_probe_scan();
-- verify the failure should be triggered once
select gp_wait_until_triggered_fault('fts_probe', 1, 1);

-- stop a mirror and show commit on dbid 2 will block
-1U: select pg_ctl((select datadir from gp_segment_configuration c where c.role='m' and c.content=0), 'stop');
-- We should insert a tuple to segment 0.
-- With jump consistent hash as the underlying hash algorithm,
-- a int value of 4 is on seg0.
0U&: insert into segwalrep_commit_blocking values (4);

-- restart primary dbid 2
-1U: select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=0), 'restart');

-- should show dbid 2 utility mode connection closed because of primary restart
0U<:
0Uq:

-- synchronous_standby_names should be set to '*' after primary restart
0U: show synchronous_standby_names;

-- this should block since mirror is not up and sync replication is on
3: begin;
3: insert into segwalrep_commit_blocking values (1);
3&: commit;

-- this should not block due to direct dispatch to primary with active synced mirror
4: insert into segwalrep_commit_blocking values (3);

-- bring the mirror back up
-1U: select pg_ctl_start(datadir, port, content, dbid) from gp_segment_configuration where role = 'm' and content = 0;

-- should unblock and commit now that mirror is back up and in-sync
3<:

-- resume FTS probes
select gp_inject_fault('fts_probe', 'reset', dbid)
    from gp_segment_configuration
    where content = -1 and role = 'p';

-- everything should be back to normal
4: insert into segwalrep_commit_blocking select i from generate_series(1,10)i;
4: select * from segwalrep_commit_blocking order by a;

-----------------------------------------------------------------
-- Master commit blocking
--
-----------------------------------------------------------------
-- set synchronous_standby_names to '*'
alter system set synchronous_standby_names to '*';

-- reload to make synchronous_standby_names effective
!\retcode gpstop -u;

-- reset connection
-- start_ignore
begin;end;
-- end_ignore

select gp_inject_fault('fts_probe', 'reset', dbid)
    from gp_segment_configuration
    where master_prober = true;
select gp_inject_fault_infinite('fts_probe', 'skip', dbid)
    from gp_segment_configuration
    where master_prober = true;

-- create table and show commits are not blocked
create table standbywalrep_commit_blocking (a int) distributed by (a);
insert into standbywalrep_commit_blocking values (1);

-- stop standby and show commit will block
select pg_ctl((select datadir from gp_segment_configuration c where c.role='m' and c.content=-1), 'stop');

-- this should block since standby is not up and sync replication is on
3: begin;
3: insert into standbywalrep_commit_blocking values (2);
3&: commit;

-- set synchronous_standby_names to ''
alter system set synchronous_standby_names to '';

-- reload to make synchronous_standby_names effective
!\retcode gpstop -u;

-- should unblock and commit now that synchronous_standby_names set to ''
3<:

-- bring the standby back up
select pg_ctl_start(datadir, port, content, dbid) from gp_segment_configuration where role='m' and content=-1;

-- set synchronous_standby_names to '*'
alter system set synchronous_standby_names to '*';

-- reload to make synchronous_standby_names effective
!\retcode gpstop -u;

-- everything should be back to normal
insert into standbywalrep_commit_blocking select i from generate_series(1,10)i;
select * from standbywalrep_commit_blocking order by a;

-- set synchronous_standby_names to default value
alter system set synchronous_standby_names to default;

-- reload to make synchronous_standby_names effective
!\retcode gpstop -u;

-- resume FTS probes
select gp_inject_fault('fts_probe', 'reset', dbid)
    from gp_segment_configuration
    where master_prober = true;
