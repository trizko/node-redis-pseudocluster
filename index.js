var net = require('net') ;
var format = require('util').format ;
var async = require('async') ;
var _ = require('underscore') ;
var through  = require('through')
var HashRing = require('hashring') ;
var shardableCommands = require('./lib/shardable') ;
var distributableCommands = require('./lib/distributable') ;
var customCommands = require('./lib/custom') ;
var commands = require("redis/lib/commands") ;
var redis_protocol = require('redis-protocol-stream')
var ProtocolFeed = require('./lib/ProtocolFeed') ;
var MultiStack = require('./lib/MultiStack') ;

var PseudoCluster = module.exports = function ( servers ) {
  
  var _this = this ;
  
  var serverKeys = this.serverKeys = servers.map(function(s){ return format("%s:%s",s.host,s.port) }) ;

  var ring = this.ring = new HashRing(serverKeys) ;

  var clientQueues = this.clientQueues = servers.map(function(server){
  
    var _ready = false ;
    var host = server.host ;
    var port = server.port ;
    var client = net.connect({port: port , host : host});
    
    client.setKeepAlive(true);
    
    client.on('connect',function(socket){ _ready = true ; });
    client.on('error',function(err){ _ready = false ; console.error( format( "%s:%s reported error:" , host , port ) , err.message ) });
    client.on('close',function(err){ 
      _ready = false ;
      console.error( "%s:%s reconnecting:" , host , port ) ; 
      setTimeout(function(){ client.connect(port,host) },1000);
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
    
    var protocolFeed = new ProtocolFeed(c) ;
    var multiStack = new MultiStack() ;
    
    protocolFeed.on('command',function( cmd , done ){
      
      _this.getReply( cmd.parsed[0] , cmd.parsed[1] , cmd.serialized , function(err,reply) {
        
        if ( err ) {
          c.write(format('-%s\r\n',err));
        } else {
          c.write(reply);
        }
        
        done();
        
      })  
      
      
    })

    protocolFeed.on('multi_start',function( cmd , done ){
      
      multiStack.drain() ;
      c.write("+OK\r\n");
      done() ;
            
    });

    protocolFeed.on('multi_discard',function( cmd , done ){
      
      multiStack.drain() ;
      c.write("+OK\r\n");
      done() ;
            
    });

    protocolFeed.on('multi_command',function( cmd , done ){
      
      _this.getReply( cmd.parsed[0] , cmd.parsed[1] , cmd.serialized , function(err,reply) {
      
        if ( err ) {
          
          multiStack.drain() ;
          c.write(format('-%s\r\n',err));
          
        } else {
          c.write("+QUEUED\r\n");
          multiStack.push(reply) ;
        }
        
        done() ;    
      
      })  
      
    });

    protocolFeed.on('multi_exec',function( cmd , done ){
      
      var results = multiStack.drain() ;
      
      c.write(format("*%s\r\n%s", results.length , results.join("") ))
      
      done();
      
    });
    
    protocolFeed.on('invalid_command',function(err,done){
      
      c.write(format('-%s\r\n',err.command_error));
      done();
      
    })

    protocolFeed.on('error',function(err,done){
      
      console.error("error",arguments);
      done();
    
    })

  });
  
}

PseudoCluster.prototype.getReply = function ( cmd , key , clientCommand , cb ) {
  
  var cmd = _.isString(cmd) && cmd.toLowerCase() ;
  
  if ( customCommands.hasOwnProperty( cmd ) ) {
    
    cb( null , customCommands[cmd](this) ) ;
    
  } else if ( distributableCommands.indexOf( cmd ) >= 0 ) {
    
    async.map( this.clientQueues , function ( clientQueue , cb ) {
      
      clientQueue.push( { cmd : clientCommand } , cb )
      
    }, function ( err , results ) {
      
      var allTokens = [] ;
      
      results.forEach(function(res,i){
        
        var tokens = tokenize(res).filter(function(token){
          
          return token.charAt(0) !== "*"
          
        }) ;
        
        allTokens = allTokens.concat(tokens) ;
        
      })
      
      allTokens.unshift(format("*%s",allTokens.length/2));
      
      var result =  allTokens.join("\r\n") + "\r\n" ;
      
      cb(null , result ) ;
      
    })
    
  } else if ( shardableCommands.indexOf( cmd ) > 0 ) {
    
    var shard = this.serverKeys.indexOf(this.ring.get( key )) ;

    var clientQueue = this.clientQueues[ shard ] ;

    clientQueue.push( { cmd : clientCommand } , function ( err , d ) {
      
      cb( null , d ) ;

    })

  } else {
    
    var err = format('NOTSUPPORTED Command "%s" not shardable.\r\n',cmd.replace(/^\s+|\s+$/g, '').toUpperCase()) ;
    
    cb( err , null ) ;

  }
  
}

function tokenize ( str ) {
  
  return str.toString().match(/(\S+|\n|\r)/g).filter(function(s){return s.replace(/^\s+|\s+$/g, '').length ;}) ;
  
}
