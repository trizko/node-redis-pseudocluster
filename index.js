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
    var _write = function(args) {
        var i;
        client.write( "*" + args.length + "\r\n");
        for (i = 0; i < args.length; i++) {
            var arg = args[i];
            client.write( "$" + arg.length + "\r\n" + arg + "\r\n");
        }
    }
    var q = async.queue(function (payload, callback) {
      
      var serialized = stringifyCommand(payload.cmd) ;

      client.once('data',function(d){
      
        callback( null , d )
      
      })
    
      if ( _ready ) {
      
        _write(payload.cmd) ;
    
      } else {
      
        client.once('connect',function(){
        
          _write(payload.cmd);
        
        });
      
      }
    
    }, 1 );
    
    client.setKeepAlive(true);
    
    client.on('connect',function(socket){ _ready = true ; });
    client.on('error',function(err){ _ready = false ; console.error( format( "%s:%s reported error:" , host , port ) , err.message ) });
    client.on('close',function(err){ 
      _ready = false ;
      console.error( "%s:%s reconnecting:" , host , port ) ; 
      setTimeout(function(){ client.connect(port,host) },1000);
    });
    
    return q ;
  
  })
  
  var server = this.server = net.createServer(function(c) { //'connection' listener
    
    var protocolFeed = new ProtocolFeed(c) ;
    var multiStack = new MultiStack() ;
    var writeReply = function(reply){
      if ( c.writable ) c.write(reply);
    }
    var writeError = function(err){
      if ( c.writable ) c.write(format('-%s\r\n',err.replace(/[\r\n]/g,'')))
    };
    
    c.on('error',function(err){ console.error(err); })
    
    protocolFeed.on('command',function( cmd , done ){
      
      _this.getReply( cmd , function(err,reply) {
        
        if ( err ) {
          writeError(err);
        } else {
          writeReply(reply);
        }
        
        done();
        
      })  
      
      
    })

    protocolFeed.on('multi_start',function( cmd , done ){
      
      multiStack.drain() ;
      writeReply("+OK\r\n");
      done() ;
            
    });

    protocolFeed.on('multi_discard',function( cmd , done ){
      
      multiStack.drain() ;
      writeReply("+OK\r\n");
      done() ;
            
    });

    protocolFeed.on('multi_command',function( cmd , done ){
      
      writeReply("+QUEUED\r\n");
      multiStack.push( cmd );
      done();
      
    });

    protocolFeed.on('multi_exec',function( execCmd , done ){
      
      var commands = multiStack.drain();
      
      async.mapSeries( commands , function ( cmd , cb ) {
        
        _this.getReply( cmd , function (err,reply) {
          
          if ( err ) {
            cb( null , format('-%s\r\n',err.replace(/[\r\n]/g,'') ) ) ;
          } else {
            cb( null , reply ) ;            
          }  
          
        });
        
      } , function ( err , results ) {
        
        if ( err ) writeError( err ) ;
        else writeReply( format("*%s\r\n%s" , results.length , results.join('') ) ) ;
        
        done()
        
      })
      
    });
    
    protocolFeed.on('invalid_command',function(err,done){
      
      writeError(err.command_error);
      done();
      
    })

    protocolFeed.on('error',function(err,done){
      
      console.error("error",arguments);
      done();
    
    })

  });
  
}

PseudoCluster.prototype.getReply = function ( cmd , cb ) {
  
  var mainCmd = _.isArray( cmd ) && cmd.length && _.isString(cmd[0]) && cmd[0].toLowerCase() || null ;
  var key = _.isArray( cmd ) && cmd.length && cmd[1] ;
  
  if ( customCommands.hasOwnProperty( mainCmd ) ) {
    
    cb( null , customCommands[mainCmd](this) ) ;
    
  } else if ( distributableCommands.indexOf( mainCmd ) >= 0 ) {
    
    async.map( this.clientQueues , function ( clientQueue , cb ) {
      
      clientQueue.push( { cmd : cmd } , cb )
      
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
    
  } else if ( shardableCommands.indexOf( mainCmd ) > 0 ) {
    
    var shard = this.serverKeys.indexOf(this.ring.get( key )) ;

    var clientQueue = this.clientQueues[ shard ] ;

    clientQueue.push( { cmd : cmd } , function ( err , d ) {
      
      cb( null , d ) ;

    })

  } else {
    
    var err = format('NOTSUPPORTED Command "%s" not shardable.',cmd.replace(/^\s+|\s+$/g, '').toUpperCase()) ;
    
    cb( err , null ) ;

  }
  
}

function tokenize ( str ) {
  
  return str.toString().match(/(\S+|\n|\r)/g).filter(function(s){return s.replace(/^\s+|\s+$/g, '').length ;}) ;
  
}

function stringifyCommand ( commandArray ) {
    
  var i , d = "" ;
  
  d += "*" + commandArray.length + "\r\n" ;

  for (i = 0; i < commandArray.length; i++) {
      var arg = commandArray[i];
      d += "$" + arg.length + "\r\n" + arg + "\r\n" ;
  }
  
  return d ;
  
}
