var util = require('util');
var stream = require('stream') ;
var Transform = stream.Transform ;
var PassThrough = stream.PassThrough ;
var through = require('through') ;
var hiredis = require("hiredis") ;
var format = require('util').format ;
var redis_protocol = require('redis-protocol-stream') ;
var EventEmitter = require('events').EventEmitter ;
var async = require('async') ;

module.exports = ProtocolFeed ;

util.inherits(FeedTransform, Transform);
util.inherits(ProtocolFeed, EventEmitter);


function ProtocolFeed ( readable ) {
  
  var _this = this ;
  
  var queue = _this.queue = async.queue(function (task, callback) {
    
    if ( task.command ) {
      
      _this.stringifyCommand( task.command.parsed , function(serialized){
      
         _this.emit( task.type , { parsed : task.command.parsed , serialized : serialized } , callback ) ;
      
      })
      
    } else {
      
      _this.emit( task.type , task , callback )
      
    }
    
    
  }, 1);
  
  var transform = new FeedTransform(queue) ;
  
  readable.pipe( transform ) ;
  
  transform.on('error',function(d){
    _this.emit('error',d)
  });

};


ProtocolFeed.prototype.stringifyCommand = function ( commandArray , cb ) {
    
  var protocol_cmd = "" ;
  var stringify = redis_protocol.stringify()
  
  stringify.on('data',function(d){ protocol_cmd += d ; });

  stringify.on('end',function(){
  
    cb(protocol_cmd) ;
  
  })
  
  stringify.write( commandArray );
  stringify.end() ;

}

function FeedTransform(queue) {
  Transform.call(this);
  this._reader = new hiredis.Reader();
  this._inMulti = false ;
  this.queue = queue ;

}

FeedTransform.prototype._transform = function(chunk, encoding, done) {
  
  var _this = this ;
  var d;
  
  if ( chunk[0] == 42 ) {
    
    _this._reader.feed( chunk ) ;
    
    while( d = _this._reader.get() ) {
    
      _this._handleParsed( d ) ;
    
    }
    
    
  } else {
    
    _this._handleParsed( tokenize(chunk) ) ;
    
  }
  
  done();

};

FeedTransform.prototype._handleParsed = function ( d ) {
  
  var _this = this ;
  var mainCmd ;
  
  if ( d && d.length ) {
    
    mainCmd = d[ 0 ].toUpperCase() ;
    
    if ( mainCmd == "MULTI" ) {
      
      _this._inMulti = true ;
      _this.queue.push({ type : 'multi_start' , command : { parsed : d } }) ;
      
    } else if ( mainCmd == "EXEC" || mainCmd == "DISCARD" ) {
      
      if ( _this._inMulti ) {
        
        if ( mainCmd == "EXEC" ) {
          
          _this.queue.push({ type : 'multi_exec' , command : { parsed : d } }) ;
          
        } else {
          
          _this.queue.push({ type : 'multi_discard' , command : { parsed : d } }) ;
          
        }
        
        _this._inMulti = false ;
        
      } else {
        
        _this.queue.push({ type : 'invalid_command' , command_error : format('%s without MULTI',mainCmd) }) ;
        
      }
      
    } else if ( _this._inMulti ) {
      
      _this.queue.push({ type : 'multi_command' , command : { parsed : d } }) ;
      
    } else {
      
      _this.queue.push({ type : 'command' , command : { parsed : d } }) ;
      
    }
    
  }
  
}

function tokenize ( str ) {
  
  return str.toString().match(/(\S+|\n|\r)/g).filter(function(s){return s.replace(/^\s+|\s+$/g, '').length ;}) ;
  
}