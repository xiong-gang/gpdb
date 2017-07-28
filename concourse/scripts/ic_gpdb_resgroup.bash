#!/bin/bash -l

set -eox pipefail

./ccp_src/aws/setup_ssh_to_cluster.sh

CLUSTER_NAME=$(cat ./cluster_env_files/terraform/name)

prepare_cgroups() {
    local gpdb_host_alias=$1
    # Requires the git Concourse resource to make the source available

    local basedir=/cgroup
    local options=rw,nosuid,nodev,noexec,relatime
    local groups="hugetlb freezer pids devices cpuset blkio net_prio net_cls cpuacct cpu memory perf_event"

    ssh -t $gpdb_host_alias "sudo bash -c '(\
        mkdir -p $basedir; \
        mount -t tmpfs tmpfs $basedir; \
        for group in $groups; do \
                mkdir -p $basedir/\$group; \
                mount -t cgroup -o $options,\$group cgroup $basedir/\$group; \
        done; \
        mkdir -p $basedir/cpu/gpdb; \
        chown -R gpadmin:gpadmin $basedir/cpu/gpdb; \
        chmod 777 $basedir/cpu/gpdb; \
        mkdir -p $basedir/cpuacct/gpdb; \
        chown -R gpadmin:gpadmin $basedir/cpuacct/gpdb; \
        chmod 777 $basedir/cpuacct/gpdb; \
    )'"
}

run_resgroup_test() {
    local gpdb_master_alias=$1
    # Requires the git Concourse resource to make the source available

    ssh $gpdb_master_alias "bash -c \"\
        source /usr/local/greenplum-db-devel/greenplum_path.sh; \
        export PGPORT=5432; \
        export MASTER_DATA_DIRECTORY=/data/master/gpseg-1; \

        cd /home/gpadmin/gpdb_src; \
        ./configure --prefix=/usr/local/greenplum-db-devel --without-zlib --without-rt --without-libcurl --without-libedit-preferred --without-docdir --without-PACKAGE --without-readline --disable-gpcloud --disable-gpfdist --disable-orca ${CONFIGURE_FLAGS}; \
        cd /home/gpadmin/gpdb_src; \
        make installcheck-resgroup; \
    \""
}
prepare_cgroups ccp-${CLUSTER_NAME}-0
prepare_cgroups ccp-${CLUSTER_NAME}-1
run_resgroup_test mdw
