var PseudoCluster = require('./index') ;

var pseudoCluster = new PseudoCluster([ { host: '127.0.0.1' , port : 6379 } , { host: '127.0.0.1' , port : 6380 } ]);

pseudoCluster.server.listen(8124,function(){
  
  console.log('listening for requests on port 8124');
  
});
