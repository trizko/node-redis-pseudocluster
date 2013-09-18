var net = require('net') ;
var format = require('util').format ;
var proxy = require('./lib/proxy') ;
var async = require('async') ;
var commands = require('redis/lib/commands') ;
var _ = require('underscore') ;
var HashRing = require('hashring') ;
var shardable = require('redis-shard/lib/shardable') ;
var redis = require('redis') ;
var Parser = require('redis/lib/parser/hiredis').Parser ;

var PseudoCluster = module.exports = function ( servers ) {
  
  var serverKeys = this.serverKeys = servers.map(function(s){ return format("%s:%s",s.host,s.port) }) ;

  var ring = this.ring = new HashRing(serverKeys) ;

  var clientQueues = this.clientQueues = servers.map(function(server){
  
    var _ready = false ;
    var host = server.host ;
    var port = server.port ;
    var client = net.connect({port: port , host : host},function(){
      
      _ready = true ;
      
    });

    var q = async.queue(function (payload, callback) {
    
      client.once('data',function(d){
      
        callback( null , d )
      
      })
    
      if ( _ready ) {
      
        client.write(payload.cmd) ;
    
      } else {
      
        client.once('connect',function(){
        
          client.write(payload.cmd);
        
        });
      
      }
    
    }, 1 );
  
    return q ;
  
  })
  
  var server = this.server = net.createServer(function(c) { //'connection' listener

    c.on('data',function(clientCommand){

      var parser = new Parser({ return_buffers : false }) ;
  
      parser.on( 'reply' , function(d){
      
        var cmd = d[0].toLowerCase() ;
        var key = d[1] ;
    
        if ( shardable.indexOf( cmd ) < 0 ) {
      
          c.write(format('-NOTSUPPORTED Command "%s" not shardable.\r\n',cmd.toUpperCase()))
      
        } else {
      
          var shard = serverKeys.indexOf(ring.get( key )) ;
      
          var clientQueue = clientQueues[ shard ] ;
      
          clientQueue.push( { cmd : clientCommand } , function ( err , d ) {
          
            c.write(d) ;
        
          })
      
        }
    
      }) ;
    
      parser.execute(clientCommand) ;
    
    })

  });
  
}