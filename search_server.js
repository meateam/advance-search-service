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
const client = new Client({ node: 'http://localhost:9200' });

async function search(call, callback) {
    try {
        const result = await client.search({
            index: 'kibana_sample_data_logs',
            body: {
                // "query": {
                //         "match": {
                //             "message": call.request.value
                //         },
                //         "match": {
                //             "host": call.request.userId
                //         },
                //         "range": {
                //             "@timestamp": {
                //               "gte": "2001-01-01T00:00:00.000+02:00",
                //               "lt": "2001-04-01T00:00:00.000+02:00"
                //             }
                //           }
                // },
                
                    "query": {
                      "bool": {
                        "must": [
                          {
                            "range": {
                              "timestamp": {
                                "gte": 633886648125,
                                "lte": 1606058904286,
                                "format": "epoch_millis"
                              }
                            }
                          },
                          {
                            "match": {
                                "message": "GET"
                            }
                          }
                        ],
                        "filter": [],
                        "should": [],
                        "must_not": []
                      }
                    },
                    "size":20,
                "_source": {
                    "includes": ["url"]
                }
            }
        })
        const fileIds = result.body.hits.hits.map(document => document._source.url)
        callback(null, { message: fileIds });
    }
    catch (err) {
        console.log(err)
        callback(err, null);
    }
}


function main() {
    var server = new grpc.Server();
    server.addService(search_proto.Search.service, { search: search });
    server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
        server.start();
    });

}

main();