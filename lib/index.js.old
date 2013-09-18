var redis = require('redis') ;
var commands = require('redis/lib/commands') ;
var _ = require('underscore') ;

exports.print = redis.print ;

exports.createClient = function ( port , host ) {
  return new ClusterClient( port , host ) ;
}

function ClusterClient ( port , host ) {
  this.port = port || 6379 ;
  this.host = host || 'localhost' ;
};

function MultiClusterClient ( port , host ) {
  this.port = port || 6379 ;
  this.host = host || 'localhost' ;
  this.queue = [] ;
};

ClusterClient.prototype.sendCommand = function ( command , args , cb ) {
  
  console.log( 'sending command %s' , command ) ;
  cb( null , [] ) ;
  
}

MultiClusterClient.prototype.addCommand = function ( command , args , cb ) {
  
  console.log( 'sending command %s' , command ) ;
  cb( null , [] ) ;
  
}


commands.forEach(function(command,i){
  
  ClusterClient.prototype[ command ] = function(){};
  MultiClusterClient.prototype[ command ] = function(){};
  ClusterClient.prototype[ command.toUpperCase() ] = function(){};
  MultiClusterClient.prototype[ command.toUpperCase() ] = function(){};
  
});

ClusterClient.prototype.multi = function ( commands ) {
  var multiClusterClient = new MultiClusterClient( this.port , this.host ) ;
  
  if ( _.isArray( commands ) ) {
    this.queue = commands ;
  }
  
  return multiClusterClient ;
  
};