-- Tests master prober selection
--
-- When the master prober segment is down, master will
-- start another master prober on another segment if
-- master and standby are in-sync.
--

include: helpers/server_helpers.sql;

create extension if not exists gp_inject_fault;

select dbid, content from gp_segment_configuration where master_prober='t';

-- stop standby
select pg_ctl(datadir, 'stop') from gp_segment_configuration where content=-1 and role='m';
-- trigger master prober
0U: select gp_request_fts_probe_scan();
-- wait master get unblocked by master prober
begin;end;
-- stop master prober segment
select pg_ctl(datadir, 'stop') from gp_segment_configuration where master_prober='t';
-- trigger manual probe 
select gp_request_fts_probe_scan();
-- we will see the master prober is not changed 
select dbid, content from gp_segment_configuration where master_prober='t';
-- start standby
select pg_ctl_start(datadir, port, content, dbid) from gp_segment_configuration where content=-1 and role='m';
-- wait standby is in-sync
do $$
begin /* in func */
  for i in 1..120 loop /* in func */
    if (select sync_state='sync' from pg_stat_replication) then /* in func */
      return; /* in func */
    end if; /* in func */
    perform gp_request_fts_probe_scan(); /* in func */
  end loop; /* in func */
end; /* in func */
$$;
-- and trigger manual probe 
select gp_request_fts_probe_scan();
-- there's a new master prober 
select dbid, content from gp_segment_configuration where master_prober='t';

-- fully recover the failed primary as new mirror
!\retcode gprecoverseg -aF;
-- trigger failover
select gp_request_fts_probe_scan();
-- expect: to see roles flipped and in sync
select content, preferred_role, role, status, mode, master_prober from gp_segment_configuration;
