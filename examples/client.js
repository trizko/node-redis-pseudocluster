var redis = require('redis') ;
var client = redis.createClient( 8124 , null , { no_ready_check : true }) ; 



//redis.debug_mode = true ;

client.sadd("mySet","foo",redis.print)
client.smembers("mySet",redis.print)
client.keys('*',redis.print)

setTimeout(function(){
  var m = client.multi() ; 
  client.sadd("mySet","foo",redis.print)
  client.smembers("mySet",redis.print)
  client.keys('*',redis.print)

  m.set("foo","bar");
  m.get("baz");
  m.exec( redis.print ) ;
  
},1)

setTimeout(function(){
  var m = client.multi() ;
  client.sadd("mySet","foo",redis.print)
  client.smembers("mySet",redis.print)
  client.keys('*',redis.print)

  m.set("foo","bar");
  m.get("baz");
  m.exec( redis.print ) ;
  
},1)

setTimeout(function(){
  
  client.sadd("mySet","foo",redis.print)
  client.smembers("mySet",redis.print)
  client.keys('*',redis.print)
  
},1)


setTimeout(function(){
  
  client.sadd("mySet","foo",redis.print)
  client.smembers("mySet",redis.print)
  client.keys('*',redis.print)
  
},2)
