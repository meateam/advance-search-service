const { Client } = require('@elastic/elasticsearch')
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const dotenv = require('dotenv');
const { Console } = require('winston/lib/winston/transports');
dotenv.config();

const SEARCH_PROTO_PATH = __dirname + '/protoFiles/search.proto';
const PERMMISSION_PROTO_PATH = __dirname + '/protoFiles/permission.proto';
const FILE_PROTO_PATH = __dirname + '/protoFiles/file.proto';

const logger = require("./logger");

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
let userId;

async function search(call, callback) {
  try {
    let indexesArray = [];
    const exactMatch = call.request.exactMatch;
    const fields = call.request.fields;
    userId = call.request.userID;

    const query = queryOrganizer(fields, exactMatch); //returns an organized Query according to the search conditions.
    const usersPermissions = await getUsersPermissions(userId);

    if (usersPermissions.permissions.length) {
      const ownersArray = await indexesCollector(usersPermissions.permissions);
      ownersArray.unshift(userId);
      indexesArray = [...new Set(ownersArray)];  //returns an Array of ownerIDs of files that were shared with the user.
    }

    const indices_boost = new Object;
    indices_boost[userId] = 2; //boost the files of the user who did the search to the top of the results.

    const result = await clientES.search({
      index: indexesArray,
      body: {
        "indices_boost": [indices_boost],
        "from": call.request.resultsAmount.from,
        "size": call.request.resultsAmount.amount,
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
        fileId: document._source.fileId,
        highlightedContent: highlighted
      };
      return fileResult;
    });

    logger.log({
      level: "info",
      message: `Search completed successfully!`,
      label: `userId: ${userId}`,
    });

    callback(null, { results: results });
  }
  catch (err) {
    logger.log({
      level: "error",
      message: `${err} `,
      label: `userId: ${userId}`,
    });
    callback(err, null);
  }
}

async function indexesCollector(permissionArray) {
  let filesArray = [];
  let ownersArray = [];
  let fileObj;

  await Promise.all(permissionArray.map(async (permission) => {
    fileObj = await getFileByID(permission.fileID);
    filesArray.push(fileObj)
  }));

  if (filesArray.includes(null)) {
    return [];
  }

  for (let file of filesArray) {
    if (file) {
      if (file.type.includes("folder")) {
        let desendantsArray = await getDesendantsById(file.id);
        if (!desendantsArray) {
          return [];
        }
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
  }

  logger.log({
    level: "info",
    message: `indexes collected successfully`,
    label: `userId: ${userId}`,
  });

  return ownersArray;
}

function getUsersPermissions(userId) {
  return new Promise((resolve, reject) => {
    permissionClient.GetUserPermissions({ userID: userId }, function (err, response) {
      if (err) {
        logger.log({
          level: "error",
          message: `in GetUserPermissions request to Drive - ${err.details}`,
          label: `userId: ${userId}`,
        });
        resolve(null);
      }
      resolve(response);
    });
  });
}

function getFileByID(fileId) {
  return new Promise((resolve, reject) => {
    fileClient.GetFileByID({ id: fileId }, function (err, response) {
      if (err) {
        logger.log({
          level: "error",
          message: `in GetFileByID request to Drive - ${err.details}`,
          label: `userId: ${userId}`,
        });
        resolve(null);
      }
      resolve(response);
    });
  });
}

function getDesendantsById(folderId) {
  return new Promise((resolve, reject) => {
    fileClient.GetDescendantsByID({ id: folderId }, function (err, response) {
      if (err) {
        logger.log({
          level: "error",
          message: `in GetDescendantsByID request to Drive - ${err.details}`,
          label: `userId: ${userId}`,
        });
        resolve(null);
      }
      resolve(response.descendants);
    });
  });
}

function queryOrganizer(fields, exactMatch) {
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
                "query": userId
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
