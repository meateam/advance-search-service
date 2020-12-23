var PROTO_PATH = __dirname + '/search.proto';

var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');

var packageDefinition = protoLoader.loadSync(
  PROTO_PATH,
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });

var search_proto = grpc.loadPackageDefinition(packageDefinition).searchService;

// Example ----->

// const fileMetaData = {
//   fileName: "crow",
//   userId: 'https://cdn.elastic-elastic-elastic.org/styles/app.css',
//   createdAt: {
//     start: null,
//     end: 5
//   },
//   updatedAt: {
//     start: 2,
//     end: null
//   },
//   type: "xl",
//   content: "sober",
//   owner: {
//     hierarchy: "kristopher",
//     name: "McKenzie",
//     userId: "027-16-6961"
//   },
//   permissions: "Gottlieb"
// }

function main() {
  var client = new search_proto.Search('localhost:8000',
    grpc.credentials.createInsecure());

  //TODO-  permission part before sending the search request
  //sending with the search request ---  1) Indexes Array of userID      2)fields (MetaData)

  client.search({ userId: 'https://cdn.elastic-elastic-elastic.org/styles/app.css', fields: fileMetaData }, function (err, response) {

    if (err) {
      return console.log('Error:', err.message);
    }

    const fileIds = response.fileIds.split(","); 
    response.fileIds=[...new Set(fileIds)];
    console.log('response: ', response);
  });
}

main();