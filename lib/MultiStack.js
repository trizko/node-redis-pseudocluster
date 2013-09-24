var util = require('util');
var through = require('through') ;
var EventEmitter = require('events').EventEmitter ;
var async = require('async') ;

module.exports = MultiReader ;

util.inherits(MultiReader, EventEmitter);


function MultiReader () {
  
  this.stack = [] ;
  
}

MultiReader.prototype.push = function( d ) {
  
  return this.stack.push( d ) ;
  
};

MultiReader.prototype.read = function() {
  
  var d = this.stack.shift() ;
  
  if ( ! this.stack.length ) {
    
    this.emit('drain') ;
    
  }
  
  return d ;
  
};

MultiReader.prototype.drain = function() {
  
  var d = [] ;
  
  while ( this.stack.length ) {
    
    d.push( this.read() )
    
  };
  
  return d ;
  
};


// Usage:
// var parser = new SimpleProtocol();
// source.pipe(parser)
// Now parser is a readable stream that will emit 'header'
// with the parsed header data.