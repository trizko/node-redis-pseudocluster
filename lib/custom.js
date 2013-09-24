var os = require('os') ;
var format = require('util').format ;
var moment = require('moment') ;

/*

redis_version:2.6.13
redis_git_sha1:00000000
redis_git_dirty:0
redis_mode:standalone
os:Darwin 12.4.0 x86_64
arch_bits:64
multiplexing_api:kqueue
gcc_version:4.2.1
process_id:597
run_id:f5d1d60ce358368f1155abd432f1dba87ad9d97e
tcp_port:6379
uptime_in_seconds:8143
uptime_in_days:0
hz:10
lru_clock:1651612

# Clients
connected_clients:3
client_longest_output_list:0
client_biggest_input_buf:0
blocked_clients:0

# Memory
used_memory:1100576
used_memory_human:1.05M
used_memory_rss:1724416
used_memory_peak:1151840
used_memory_peak_human:1.10M
used_memory_lua:31744
mem_fragmentation_ratio:1.57
mem_allocator:libc

# Persistence
loading:0
rdb_changes_since_last_save:2
rdb_bgsave_in_progress:0
rdb_last_save_time:1379656785
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:-1
rdb_current_bgsave_time_sec:-1
aof_enabled:0
aof_rewrite_in_progress:0
aof_rewrite_scheduled:0
aof_last_rewrite_time_sec:-1
aof_current_rewrite_time_sec:-1
aof_last_bgrewrite_status:ok

# Stats
total_connections_received:35
total_commands_processed:30
instantaneous_ops_per_sec:0
rejected_connections:0
expired_keys:0
evicted_keys:0
keyspace_hits:0
keyspace_misses:0
pubsub_channels:0
pubsub_patterns:0
latest_fork_usec:0

# Replication
role:master
connected_slaves:0

# CPU
used_cpu_sys:1.75
used_cpu_user:1.36
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Keyspace
db0:keys=2,expires=0

*/


exports.info = function ( inst , args ) {
  
  var writeLine = format.bind(format,"%s:%s") ;
  
  var d =  [
    "# Server" ,
    writeLine( "redis_version" , "0.0.1 PseudoCluster" ) ,
    writeLine( "redis_mode" , "pseudocluster" ) ,
    writeLine( "redis_version" , "0.0.1 PseudoCluster" ) ,
    writeLine( "os" , format( "%s %s %s" , os.type() , os.release() , os.arch() ) ) ,
    writeLine( "arch_bits" , os.arch().search(/[0-9]+/) && os.arch().match(/[0-9]+/)[0] || 32 ) ,
    writeLine( "multiplexing_api" , "kqueue" ) ,
    writeLine( "process_id" , process.pid ) ,
    writeLine( "tcp_port" , inst.server.address().port ) ,
    writeLine( "uptime_in_seconds" , process.uptime() ) ,
    writeLine( "uptime_in_days" , Math.floor(process.uptime()/86400) ) ,
    ''
  
  ]; ;
  
  var resp = d.join("\r\n") ;
  
  return format("$%s\r\n%s\r\n" , resp.length , resp ) ;
  
};

exports.ping = function ( inst , args ) {
  
  var resp = "PONG" ;
  
  return format("$%s\r\n%s\r\n" , resp.length , resp ) ;
  
};


exports.mset = function ( inst , args ) {
  
  var resp = "OK" ;
  
  return format("$%s\r\n%s\r\n" , resp.length , resp ) ;
  
};
