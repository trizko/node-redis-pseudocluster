var redis = require('redis') ;
// must not send INFO cmd with ready check. not supported because it's not shardable
var client = redis.createClient( 8124 , null , { no_ready_check : true } ) ; 
var async = require('async') ;

redis.debug_mode = true ;


var m = client.multi() ;

m.set("foo","bar");
m.get("baz");

m.exec(function(){
  
  console.log(arguments);
  
})
//client.keys('*',redis.print)