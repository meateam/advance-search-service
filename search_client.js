var PROTO_PATH = __dirname + '/search.proto';

var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');
var packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {keepCase: true,
     longs: String,
     enums: String,
     defaults: true,
     oneofs: true
    });
var search_proto = grpc.loadPackageDefinition(packageDefinition).searchService;

function main() {
    var client = new search_proto.Search('localhost:50051',
                                         grpc.credentials.createInsecure());


    client.search({userId: 'https://cdn.elastic-elastic-elastic.org/styles/app.css', fileds:["a", "b"], value: 'GET'}, function(err, response) {
      if(err){
        return console.log('Error:', err.message);
      }
        console.log('Found:', response.message);
      });
  }

  main();