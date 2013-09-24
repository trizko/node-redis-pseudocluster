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
      
      _this.emit( task.type , task.command , callback ) ;
      
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

FeedTransform.prototype._handleParsed = function ( parsedCmd ) {
  
  var _this = this ;
  var mainCmd ;
  
  if ( parsedCmd && parsedCmd.length ) {
    
    mainCmd = parsedCmd[ 0 ].toUpperCase() ;
    
    if ( mainCmd == "MULTI" ) {
      
      _this._inMulti = true ;
      _this.queue.push({ type : 'multi_start' , command : parsedCmd }) ;
      
    } else if ( mainCmd == "EXEC" || mainCmd == "DISCARD" ) {
      
      if ( _this._inMulti ) {
        
        if ( mainCmd == "EXEC" ) {
          
          _this.queue.push({ type : 'multi_exec' , command : parsedCmd }) ;
          
        } else {
          
          _this.queue.push({ type : 'multi_discard' , command : parsedCmd }) ;
          
        }
        
        _this._inMulti = false ;
        
      } else {
        
        _this.queue.push({ type : 'invalid_command' , command_error : format('%s without MULTI',mainCmd) }) ;
        
      }
      
    } else if ( _this._inMulti ) {
      
      _this.queue.push({ type : 'multi_command' , command : parsedCmd }) ;
      
    } else {
      
      _this.queue.push({ type : 'command' , command : parsedCmd }) ;
      
    }
    
  }
  
}

function tokenize ( str ) {
  
  return str.toString().match(/(\S+|\n|\r)/g).filter(function(s){return s.replace(/^\s+|\s+$/g, '').length ;}) ;
  
}