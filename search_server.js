const { Client } = require('@elastic/elasticsearch')
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const dotenv = require('dotenv');
dotenv.config();

const SEARCH_PROTO_PATH = __dirname + '/protoFiles/search.proto';
const PERMMISSION_PROTO_PATH = __dirname + '/protoFiles/permission.proto';
const FILE_PROTO_PATH = __dirname + '/protoFiles/file.proto';

const conditions = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
};

const searchPackageDefinition = protoLoader.loadSync(
  SEARCH_PROTO_PATH,
  conditions);
const permissionPackageDefinition = protoLoader.loadSync(
  PERMMISSION_PROTO_PATH,
  conditions);
const filePackageDefinition = protoLoader.loadSync(
  FILE_PROTO_PATH,
  conditions);

const search_proto = grpc.loadPackageDefinition(searchPackageDefinition).searchService;
const permission_proto = grpc.loadPackageDefinition(permissionPackageDefinition).permission;
const file_proto = grpc.loadPackageDefinition(filePackageDefinition).file;

const clientES = new Client({ node: `${process.env.ES_URL}` });

const permissionClient = new permission_proto.Permission(`${process.env.PERMISSION_SERVICE_URL}`,
  grpc.credentials.createInsecure());

const fileClient = new file_proto.FileService(`${process.env.FILE_SERVICE_URL}`,
  grpc.credentials.createInsecure());

function main() {
  var server = new grpc.Server();
  server.addService(search_proto.Search.service, { search: search });
  server.bindAsync(`${process.env.SEARCH_URL}`, grpc.ServerCredentials.createInsecure(), () => {
    server.start();
  });
}

main();

async function search(call, callback) {
  try {
    let highlight;
    const exactMatch = call.request.exactMatch;
    const fields = call.request.fields;

    const query = timeOrganizer(fields, exactMatch); //returns an organized Query according to the search conditions.
    const usersPermissions = await getUsersPermissions(call.request.userID);
    const ownersArray = await indexesCollector(usersPermissions.permissions);
    ownersArray.push(call.request.userID);
    const indexesArray = [...new Set(ownersArray)];  //returns an Array of ownerIDs of files that were shared with me.

    const result = await clientES.search({
      index: indexesArray,
      body: {
        "from": 0,
        "size": process.env.MAX_RESULT,  //max number of results. (default is 10)
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
        ,
        "collapse": {   //return uniqe file Ids-(one result from each file)
          "field": "fileId.keyword"
        }
      }
    }
    );


    const results = await result.body.hits.hits.map(document => {
      let highlighted = null;
      if (fields.content) {
        highlighted = document.highlight.content[0];
      };
      const fileResult = {
        fileIDs: document._source.fileId,
        highlightedContent: highlighted
      };
      return fileResult;
    });
    console.log(results)

    callback(null, { results: results });
  }
  catch (err) {
    console.log(err)
    callback(err, null);
  }
}

function getUsersPermissions(userId) {
  return new Promise((resolve, reject) => {
    permissionClient.GetUserPermissions({ userID: userId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response)
    });
  });
}
async function indexesCollector(permissionArray) {
  let filesArray = [];
  let ownersArray = [];
  let fileObj;

  await Promise.all(permissionArray.map(async (permission) => {
    fileObj = await getFileByID(permission.fileID);
    filesArray.push(fileObj)
  }));


  for (let file of filesArray) {
    if (file.type.includes("folder")) {

      let desendantsArray = await getDesendantsById(file.id); //DRIVE --- GetDescendantsByID
      let ownersIds = desendantsArray.map((desendants) => {
        return desendants.file.ownerID;
      });

      ownersIds.push(file.ownerID)
      ownersArray = ownersArray.concat(ownersIds);
    }
    else if (file.type.includes("document")) {
      ownersArray.push(file.ownerID);
    }
  }

  return ownersArray;
}

function getFileByID(fileId) {
  return new Promise((resolve, reject) => {
    fileClient.GetFileByID({ id: fileId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response)
    });
  });
}

function getDesendantsById(folderId) {
  return new Promise((resolve, reject) => {
    fileClient.GetDescendantsByID({ id: folderId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response.descendants)
    });
  });
}

function timeOrganizer(fields, exactMatch) {
  let searchType;

  if (exactMatch) {   //type of content search
    searchType = {
      "match_phrase": {
        "content": fields.content
      }
    }
  }
  else {
    searchType = {
      "query_string": {
        "default_field": "content",
        "query": ` ${fields.content}*`
      }
    }
  }

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
    searchType,
    {
      "nested": {
        "path": "permissions",
        "query": {
          "bool": {
            "must": [{
              "bool": {
                "should": [{
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
                }]
              }
            },
            {
              "query_string": {
                "default_field": "permissions.user.userId",
                "query": `*${fields.userId}*`
              }
            }
            ]

          }
        }
      }
    }
  ];

  if (fields.owner) {
    const ownerHierarchy =
    {
      "query_string": {
        "default_field": "owner.hierarchy",
        "query": `*${fields.owner.hierarchy}*`
      }
    };
    const ownerName = {
      "query_string": {
        "default_field": "owner.name",
        "query": `*${fields.owner.name}*`
      }
    };

    query.push(ownerHierarchy);
    query.push(ownerName)
  }

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
  const oldest = new Date(process.env.OLDEST_YEAR, process.env.OLDEST_MONTH, process.env.OLDEST_DAY).getTime().toString();
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
