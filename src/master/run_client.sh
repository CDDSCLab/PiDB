#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../shflags


# define command-line flags
DEFINE_integer server_port 8300 "Port of the first server"
DEFINE_integer server_num '3' 'Number of servers'
DEFINE_string valgrind 'false' 'Run in valgrind'

FLAGS "$@" || exit 1

# hostname prefers ipv6
IP=`hostname -i | awk '{print $NF}'`

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

raft_peers=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    raft_peers="${raft_peers}${IP}:$((${FLAGS_server_port}+i)):0,"
done

export TCMALLOC_SAMPLE_PARAMETER=524288

${VALGRIND} ./client \
        -conf="${raft_peers}" \
