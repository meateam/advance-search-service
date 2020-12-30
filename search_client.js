var SPROTO_PATH = __dirname + '/search.proto';
var PPROTO_PATH = __dirname + '/permission.proto';
var FPROTO_PATH = __dirname + '/file.proto';


var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');

const cond = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
};

var spackageDefinition = protoLoader.loadSync(
  SPROTO_PATH,
  cond);

var ppackageDefinition = protoLoader.loadSync(
  PPROTO_PATH,
  cond);

var fpackageDefinition = protoLoader.loadSync(
  FPROTO_PATH,
  cond);

var search_proto = grpc.loadPackageDefinition(spackageDefinition).searchService;
var permission_proto = grpc.loadPackageDefinition(ppackageDefinition).permission;
var file_proto = grpc.loadPackageDefinition(fpackageDefinition).file;

// Example ----->

const fileMetaData = {
  fileName: "",
  userId: '',
  type: "",
  content: "summer",
  owner: {
    hierarchy: "",
    name: "",
    userId: ""
  },
  permissions: ""
}

async function main() {
  const sclient = new search_proto.Search('localhost:8005',
    grpc.credentials.createInsecure());

  const pclient = new permission_proto.Permission('137.135.166.218:8087',
    grpc.credentials.createInsecure());

  let userH = "5eeb9011eaa6861b4a2138fc";

  let oA = await getUsersPermissions(pclient, userH);
  let ownersArray = await indexesCollector(oA.permissions)
  ownersArray.push(userH)
  console.log(ownersArray)

  console.log("hghgfhf");

  let results = await search(sclient, ownersArray)
  console.log("results");
  console.log(results);

}

main();

async function search(sclient, user) {
  return new Promise((resolve, reject) => {
    sclient.search({ ownersArray: user, exactMatch: true, fields: fileMetaData }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response)
    });
  });
}


async function indexesCollector(permissionArray) {
  let filesArray = [];
  let  = [];
  let ownersArray = [];
  let file;

  const fclient = new file_proto.FileService('137.135.166.218:8083',
    grpc.credentials.createInsecure());

  await Promise.all(permissionArray.map(async (permission) => {
    file = await getFileByID(fclient, permission.fileID);
    filesArray.push(file)
  }));

  console.log("filesArray.length: " + filesArray.length)

  let j = filesArray.map((x) => { return x.name })


  for (let v of filesArray) {
    if (v.type.includes("folder")) {

      let d = await getDesendantsById(fclient, v.id); //DRIVE --- GetDescendantsByID
      let h = d.map((desendants) => {
        return desendants.file.name;
      });

      h.push(v.name)
      ownersArray = ownersArray.concat(h);
    }
    else if (v.type.includes("document")) {
      ownersArray.push(v.name);
    }
  }

  return ownersArray;
}


function getFileByID(fclient, fileId) {
  return new Promise((resolve, reject) => {
    fclient.GetFileByID({ id: fileId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response)
    });
  });
}


function getUsersPermissions(pclient, userId) {
  return new Promise((resolve, reject) => {
    pclient.GetUserPermissions({ userID: userId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response)
    });
  });
}


function getDesendantsById(fclient, folderId) {
  return new Promise((resolve, reject) => {
    fclient.GetDescendantsByID({ id: folderId }, function (err, response) {
      if (err) {
        reject(err);
      }
      resolve(response.descendants)
    });
  });
}



