var net = require('net') ;
var format = require('util').format ;
var async = require('async') ;
var _ = require('underscore') ;
var through  = require('through')
var HashRing = require('hashring') ;
var shardable = require('./lib/shardable') ;
var distributable = require('./lib/distributable') ;
var commands = require("redis/lib/commands") ;
var hiredis = require("hiredis");

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
    
    client.on('connect',function(socket){ console.log("connected to redis server at %s:%s" , host , port ) ; _ready = true ; });
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
    
    var inMulti = false ;
    var multiReplies = [] ;
    var multiResults = [] ;
    var multiCommands = [] ;
    var discard = false ;
    var multiIncrementalResponseTimeout;
    var replyTimeout = function(){
      
      multiIncrementalResponseTimeout = setTimeout(function(){
        
        var reply = multiReplies.shift() ;
        
        if ( reply ) c.write( reply ) ;
        
      },10)
      
    };
    
    
    c.on('data',function(clientCommand){

      var fullCmd = parseClientCommand( clientCommand ) ;
      var hasCommands = fullCmd.length && fullCmd[0].length ;
      var firstCommand = hasCommands && fullCmd[0][0] ;
      var lastCommand = hasCommands && fullCmd[ fullCmd.length-1 ][ fullCmd[ fullCmd.length-1 ].length-1 ]
      var isMultiHeader = firstCommand && firstCommand.toLowerCase() == "multi" ;
      var isMultiFooter = lastCommand && lastCommand.toLowerCase() == "exec" ;
      var isDiscard = firstCommand && firstCommand.toLowerCase() == "discard" ;
      
      inMulti = isMultiHeader || inMulti ;
      discard = isDiscard || discard ;
      
      if ( ! inMulti ) {
        
        _this.getReply( fullCmd[0][0] , fullCmd[0][1] , clientCommand , function(err,reply){
          
          if ( err ) {
            c.write(format("-%s",err));
          } else {
            c.write(reply);            
          }
        
        })
        
      } else {
        
        clearTimeout( multiIncrementalResponseTimeout ) ;
        
        fullCmd.forEach(function(cmdArray){
          
          var newCmdArr = cmdArray.filter(function(cmd){
            
            return _.isString(cmd) && cmd.toLowerCase() !== "multi" && cmd.toLowerCase() !== "exec"
            
          }) ;
          
          if ( newCmdArr.length ) {
            
            multiReplies.push("+QUEUED\r\n") ;
            multiCommands.push( newCmdArr ) ;
            replyTimeout();
                        
          }
          
        })
        
        if ( isMultiHeader && ! multiCommands.length ) {
          
          multiReplies.push("+OK\r\n");
          replyTimeout();
          
        } else if ( isMultiHeader && multiCommands.length ) {
          
          discard = true ;
          multiReplies.push("-ERR wrong number of arguments for 'multi' command\r\n");
          replyTimeout();
          
        } else if ( isMultiFooter && discard ) {
          
          var response = multiReplies.concat(["-ERR multi discarded\r\n"]).join("")
          
          c.write(response);
          
          inMulti = false ;
          discard = false ;
          multiReplies = [] ;
          multiCommands = [] ;
          multiResults = [] ;
          
          
        } else if ( isMultiFooter && ! discard ) {
          
          multiResults.push(format("*%s\r\n",multiCommands.length)) ;
          
          async.map( multiCommands , function ( multiCommandArr , cb ){
            
            var command = format("*%s\r\n",multiCommandArr.length) ;
            
            multiCommandArr.forEach(function(token){
              
              command += format("$%s\r\n%s\r\n",token.length,token) ;
              
            })
            
            _this.getReply( multiCommandArr[0] , multiCommandArr[1] , command , function(err,reply){
              
              if ( err ) {
                cb(null,format("-%s",err));
              } else {
                cb(null,reply)
              }
              
            });
            
          } , function(err,results) {
            
            var response = multiReplies.concat(multiResults).concat(results).join("")
            
            c.write(response);
            
            inMulti = false ;
            discard = false ;
            multiReplies = [] ;
            multiCommands = [] ;
            multiResults = [] ;
            
            
          })
          
        }
        
      }
    
    })

  });
  
}

PseudoCluster.prototype.getReply = function ( cmd , key , clientCommand , cb ) {
  
  var cmd = _.isString(cmd) && cmd.toLowerCase() ;
  
  if ( cmd == "exec" ) {
   
    var err = "ERR EXEC without MULTI\r\n"
    
    cb( err , null );
    
  } else if ( distributable.indexOf( cmd ) >= 0 ) {
    
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
    
  } else if ( shardable.indexOf( cmd ) > 0 ) {
    
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

function parseClientCommand ( clientCommand ) {
  
  var fullCmd = [] ;
  
  if ( clientCommand.toString().charAt(0) == "*" ) {
    
    var stillData = true ;
    var reader = new hiredis.Reader();
    reader.feed(clientCommand) ;

    while ( stillData ) {
    
      var part = reader.get() ;
    
      if ( part ) {
      
        fullCmd.push(part) ;
      
      } else {
      
        stillData = false ;
      
      }
    
    }
    
  } else {
    
    var tokens = tokenize(clientCommand);
    
    tokens.forEach(function(token){
      
      if ( commands.indexOf( token.toLowerCase() ) >=0 ) {
        
        fullCmd.push([]) ;
        
      }
      
      if ( fullCmd.length ) {
        
        fullCmd[fullCmd.length-1].push( token ) ;
        
      }
      
    })
    
  }
  
  return fullCmd ;
  
}

function tokenize ( str ) {
  
  return str.toString().match(/(\S+|\n|\r)/g).filter(function(s){return s.replace(/^\s+|\s+$/g, '').length ;}) ;
  
}
