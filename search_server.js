const PROTO_PATH = __dirname + '/search.proto';
const { Client } = require('@elastic/elasticsearch')
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });
const search_proto = grpc.loadPackageDefinition(packageDefinition).searchService;
const clientES = new Client({ node: 'http://localhost:9200' });


function main() {
  var server = new grpc.Server();
  server.addService(search_proto.Search.service, { search: search });
  server.bindAsync('0.0.0.0:8000', grpc.ServerCredentials.createInsecure(), () => {
    server.start();
  });
}

main();

async function search(call, callback) {
  try {
    let fields = call.request.fields;
    console.log(fields);
    let indexesArray = new Array("liora", "lior2");
    console.log(indexesArray);

    const result = await clientES.search({
      index: indexesArray,
      body: {
        "from": 0, "size": 20,  //max number of results 
        "query": {
          "bool": {
            "must": [
              {
                "query_string": {
                  "default_field": "fileName",
                  "query": `*${fields.fileName}*`
                }
              },
              {
                "query_string": {
                  "default_field": "type",
                  "query": `*${fields.type}*`
                }
              },
              {
                "query_string": {
                  "default_field": "owner.name",
                  "query": `*${fields.owner.name}*`
                }
              },
              {
                "query_string": {
                  "default_field": "owner.hierarchy",
                  "query": `*${fields.owner.hierarchy}*`
                }
              },
              {
                "query_string": {
                  "default_field": "content",
                  "query": `* ${fields.content}*`
                }
              },
              {
                "range": {
                  "createdAt": {
                    "gte": `${fields.createdAt.start}`,
                    "lte": `${fields.createdAt.end}`
                  }
                }
              },
              {
                "range": {
                  "updatedAt": {
                    "gte": `${fields.updatedAt.start}`,
                    "lte": `${fields.updatedAt.end}`
                  }
                }
              },
              {
                "nested": {
                  "path": "permissions",
                  "query": {
                    "bool": {
                      "should": [
                        {
                          "query_string": {
                            "default_field": "permissions.user.hierarchy",
                            "query": `*${fields.permissions}*`
                          }
                        },
                        {
                          "query_string": {
                            "default_field": "permissions.user.name",
                            "query": `*${fields.permissions}*`
                          }
                        }
                      ]
                    }
                  }
                }
              }
            ]
          }
        },
        "highlight": {
          "pre_tags": ["<b>"],
          "post_tags": ["</b>"],
          "fields": {
            "content": {}
          }
        }
      }
    }
    );

    const fileIds = result.body.hits.hits.map(document =>  document._source.fileId);

    const highlight = result.body.hits.hits.map(document => {
      if (document.highlight) {
        return document.highlight.content;
      }
    }).join(' ');

    // console.log("fileIds: " + fileIds);
    // console.log("highlight: " + highlight);

    callback(null, { fileIds: fileIds, highlightedContent:highlight });
  }
  catch (err) {
    console.log(err)
    callback(err, null);
  }
}