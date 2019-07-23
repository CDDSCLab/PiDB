# created by blue 2019/6/2

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../shflags

# define command-line flags
DEFINE_string valgrind 'false' 'Run in valgrind'
DEFINE_integer server_num '3' 'Number of servers'
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_integer port 8300 "Port of the first server"

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

# The alias for printing to stderr
alias error=">&2 echo hehe: "

# hostname prefers ipv6
IP=`hostname -i | awk '{print $NF}'`

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

raft_peers=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    raft_peers="${raft_peers}${IP}:$((${FLAGS_port}+i)):0,"
done

if [ "$FLAGS_clean" == "1" ]; then
    rm -rf runtime
fi

export TCMALLOC_SAMPLE_PARAMETER=524288

for ((i=0; i<$FLAGS_server_num; ++i)); do
    mkdir -p runtime/$i
    cp ./server runtime/$i
    cp ../head/route_table.json runtime/$i
    cp ../head/store_table.json runtime/$i
    cd runtime/$i
    ${VALGRIND} ./server \
        -port=$((${FLAGS_port}+i)) -conf="${raft_peers}" > std.log 2>&1 &

    cd ../..
done
