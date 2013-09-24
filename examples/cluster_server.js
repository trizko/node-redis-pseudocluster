var cluster = require('cluster');
var http = require('http');
var PseudoCluster = require('../index') ;

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < 2; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
} else {

  var pseudoCluster = new PseudoCluster([ { host: '127.0.0.1' , port : 6379 } , { host: '127.0.0.1' , port : 6380 } ]);

  // Workers can share any TCP connection
  pseudoCluster.server.listen(8124,function(){
  
    console.log('worker listening on port 8124');
  
  });
  
}