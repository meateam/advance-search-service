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
    const fields = call.request.fields;
    const indexesArray = new Array("liora", "lior2");
    const query = timeOrganizer(fields);

    const result = await clientES.search({
      index: indexesArray,
      body: {
        "from": 0, "size": 20,  //max number of results 
        "query": {
          "bool": {
            "must": query
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

    const fileIds = result.body.hits.hits.map(document => document._source.fileId);

    const highlight = result.body.hits.hits.map(document => {
      if (document.highlight) {
        return document.highlight.content;
      }
    }).join(' ');

    callback(null, { fileIds: fileIds, highlightedContent: highlight });
  }
  catch (err) {
    console.log(err)
    callback(err, null);
  }
}


function timeOrganizer(fields) {
  const query = [
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
  ];

  if (fields.updatedAt) {
    const updatedAt = {
      "range": {
        "updatedAt": {
          "gte": fields.updatedAt.start,
          "lte": fields.updatedAt.end
        }
      }
    };
    pushToQuery(fields.updatedAt, query, updatedAt);
  }

  if (fields.createdAt) {
    const createdAt = {
      "range": {
        "createdAt": {
          "gte": fields.createdAt.start,
          "lte": fields.createdAt.end
        }
      }
    };
    pushToQuery(fields.createdAt, query, createdAt);
  }

  return query;
}

function pushToQuery(field, query, rangeQuery) {
  const oldest = new Date(2000, 0, 1).getTime().toString();
  const newest = Date.now().toString();
  const fieldName = Object.keys(rangeQuery.range)[0];

  if (field.start && field.end) {
    query.push(rangeQuery);
  }
  else if (field.start || field.end) {
    if (!field.end) {
      field.end = newest;
      rangeQuery.range[fieldName].lte = field.end
    }

    if (!field.start) {
      field.start = oldest;
      rangeQuery.range[fieldName].gte = field.start
    }

    query.push(rangeQuery);
  }
}