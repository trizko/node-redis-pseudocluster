var net = require('net') ;
var format = require('util').format ;
var proxy = require('./lib/proxy') ;
var async = require('async') ;
var commands = require('redis/lib/commands') ;
var redis_protocol = require('redis-protocol-stream') ;
var _ = require('underscore') ;
var HashRing = require('hashring') ;
var unshardable = require('redis-shard/lib/unshardable') ;
var redis = require('redis') ;
var servers = [ '127.0.0.1:6379', '127.0.0.1:6380' ] ;
var ring = new HashRing(servers) ;
var clientQueues = servers.map(function(server){
  
  var _ready = false ;
  var host = server.split(':')[0] ;
  var port = server.split(':')[1] ;
  var client = net.connect({port: port , host : host},function(){
      
    _ready = true ;
      
  });

  var q = async.queue(function (payload, callback) {

    var protocol_serialize = redis_protocol.stringify() ;
    
    client.once('data',function(d){
      
      callback( null , d )
      
    })
    
    client.write(payload.cmd) ;
    
  }, 1 );
  
  return q ;
  
})

var server = net.createServer(function(c) { //'connection' listener

  c.on('data',function(clientCommand){

    var protocol_parse = redis_protocol.parse({ return_buffers : false }) ;
  
    protocol_parse.on( 'data' , function(d){
      var cmd = d[0].toLowerCase() ;
      var key = d[1] ;
    
      if ( unshardable.indexOf( cmd ) >= 0 ) {
      
        c.write(format('-NOTSUPPORTED Command "%s" not shardable.\r\n',cmd.toUpperCase()))
      
      } else {
      
        var shard = servers.indexOf(ring.get( key )) ;
      
        var clientQueue = clientQueues[ shard ] ;
      
        clientQueue.push( { cmd : clientCommand } , function ( err , d ) {
          
          c.write(d) ;
        
        })
      
      }
    
    }) ;
    
    protocol_parse.write(clientCommand) ;
    
  })
  
/*
  console.log('server connected');
  c.on('end', function() {
    console.log('server disconnected');
  });
  c.write('hello\r\n');
  c.pipe(c);
*/

});
server.listen(8124, function() { //'listening' listener
  console.log('listening for connections on port 8124');
});
