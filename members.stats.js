var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var Promise = require('bluebird');

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');
var loader = require('./reuse/loader');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
var dynamoDB;
var dynamoDBDocC;
var elasticClient;
var esMemberStatsIndices;
var esMemberStatsMappings;
if (args.indexOf("dev") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    requestTimeout : Infinity,
    keepAlive: true,
    sniffOnStart: false
    //log: 'trace'
  });
  esMemberStatsIndices = esConfig.dev.esMemberStatsIndices
  esMemberStatsMappings = esConfig.dev.esMemberStatsMappings
} else if (args.indexOf("prod") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
  AWS.config.loadFromPath('./config/aws-prod-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.prod.esHost,
    requestTimeout : Infinity,
    keepAlive: true,
    sniffOnStart: false
    //log: 'trace'
  });
  esMemberStatsIndices = esConfig.prod.esMemberStatsIndices
  esMemberStatsMappings = esConfig.prod.esMemberStatsMappings
} else {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    requestTimeout : Infinity,
    keepAlive: true,
    sniffOnStart: false
    //log: 'trace'
  });
  esMemberStatsIndices = esConfig.dev.esMemberStatsIndices
  esMemberStatsMappings = esConfig.dev.esMemberStatsMappings
}

var countStats = [];
var checkStats = [];

// Prod  - public - (TT 00:00:00:30:463 / ETC 00:00:55:20:024)
// Prod  - private - (TT 00:00:00:13:202 / ETC 00:00:00:00:000)
// Dev - public - (TT 00:00:00:30:954 / ETC 00:00:30:49:257)
// Dev - private - (TT 00:00:00:06:282 / ETC 00:00:00:00:000)
var limitStats = 100000;
var statsLastEvaluatedKeyArray = [{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0}]

var startTime;
var fullFilePath;
var userIdsCompleted;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberStatsPublic(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, totalItemCount) {
  const memberStatsPublicParams = {
    TableName: 'MemberStats',
    Limit: limitStats,
    ExclusiveStartKey: lastEvaluatedKey,
    Segment: segment,
    TotalSegments: totalSegments
  }
  try {
    const membersStatsPublic = await dynamoDBDocC.scan(memberStatsPublicParams).promise();
    if (membersStatsPublic != null) {
      var esData = []
      for (let masIndex = 0; masIndex < membersStatsPublic.Items.length; masIndex++) {
        countStats[segment] = countStats[segment] + 1;
        var setPublicId = membersStatsPublic.Items[masIndex].userId + "_10";
        if (!util.exists(userIdsCompleted, setPublicId)) {
          membersStatsPublic.Items[masIndex].groupId = 10
          if (membersStatsPublic.Items[masIndex].hasOwnProperty("maxRating")) {
            membersStatsPublic.Items[masIndex].maxRating = JSON.parse(membersStatsPublic.Items[masIndex].maxRating)
          }
          if (membersStatsPublic.Items[masIndex].hasOwnProperty("DATA_SCIENCE")) {
            membersStatsPublic.Items[masIndex].DATA_SCIENCE = JSON.parse(membersStatsPublic.Items[masIndex].DATA_SCIENCE)
          }
          if (membersStatsPublic.Items[masIndex].hasOwnProperty("DESIGN")) {
            membersStatsPublic.Items[masIndex].DESIGN = JSON.parse(membersStatsPublic.Items[masIndex].DESIGN)
          }
          if (membersStatsPublic.Items[masIndex].hasOwnProperty("DEVELOP")) {
            membersStatsPublic.Items[masIndex].DEVELOP = JSON.parse(membersStatsPublic.Items[masIndex].DEVELOP)
          }
          if (membersStatsPublic.Items[masIndex].hasOwnProperty("COPILOT")) {
            membersStatsPublic.Items[masIndex].COPILOT = JSON.parse(membersStatsPublic.Items[masIndex].COPILOT)
          }
          esData.push({ index: { _index: esMemberStatsPublicIndices, _type: esMemberStatsPublicMappings, _id: setPublicId } })
          esData.push(util.cleanse(membersStatsPublic.Items[masIndex]))
          
          util.add(userIdsCompleted, setPublicId)
          loader.display(loader.MESSAGES.ONLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          console.log(loader.MESSAGES.SKIP + " --> setPublicId == " +  setPublicId)
          loader.display(loader.MESSAGES.SKIP, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if(esData.length > 0) {
        let esResponse = await myElasticSearch.bulkToIndex(elasticClient, esData, false, 1)
        if (esResponse.errors == false) {
          loader.display(loader.MESSAGES.ESUPDATED, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.ESOFFLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }

      if (membersStatsPublic.LastEvaluatedKey) {
        statsLastEvaluatedKeyArray[segment].key = membersStatsPublic.LastEvaluatedKey;
        statsLastEvaluatedKeyArray[segment].count = countStats[segment];
        scanMemberStatsPublic(membersStatsPublic.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, totalItemCount)
      } else {
        loader.display(loader.MESSAGES.COMPLETED, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
        checkStats[segment] = true;
        if (checkStats.every(util.isTrue)) {
          startTime = moment().format("DD-MM-YYYY HH:mm:ss");
          util.durationTaken("Write to file (" + fullFilePath + ") - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
          fs.writeFile(fullFilePath, JSON.stringify(userIdsCompleted), function (err) {
            if (err) {
              return console.log(err);
            }
            util.durationTaken("Write to file (" + fullFilePath + ") - End -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
          });
        }
      }
    }
  } catch (err) {
    // console.log(err)
    loader.display(loader.MESSAGES.DBOFFLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
    setTimeout(function () {
      loader.display(loader.MESSAGES.REVOKE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
      scanMemberStatsPublic(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, totalItemCount)
    }, 5000);
  }
}

async function scanMemberStatsPrivate(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, totalItemCount) {
  const memberStatsPrivateParams = {
    TableName: 'MemberStats_Private',
    Limit: limitStats,
    ExclusiveStartKey: lastEvaluatedKey,
    Segment: segment,
    TotalSegments: totalSegments
  }
  try {
    const membersStatsPrivate = await dynamoDBDocC.scan(memberStatsPrivateParams).promise();
    if (membersStatsPrivate != null) {
      var esData = []
      for (let masIndex = 0; masIndex < membersStatsPrivate.Items.length; masIndex++) {
        countStats[segment] = countStats[segment] + 1;
        var setPrivateId = membersStatsPrivate.Items[masIndex].userId + "_" + membersStatsPrivate.Items[masIndex].groupId;
        if (!util.exists(userIdsCompleted, setPrivateId)) {
          if (membersStatsPrivate.Items[masIndex].hasOwnProperty("maxRating")) {
            membersStatsPrivate.Items[masIndex].maxRating = JSON.parse(membersStatsPrivate.Items[masIndex].maxRating)
          } else {
            membersStatsPrivate.Items[masIndex].maxRating = {}
          }
          if (membersStatsPrivate.Items[masIndex].hasOwnProperty("DATA_SCIENCE")) {
            membersStatsPrivate.Items[masIndex].DATA_SCIENCE = JSON.parse(membersStatsPrivate.Items[masIndex].DATA_SCIENCE)
          } else {
            membersStatsPrivate.Items[masIndex].DATA_SCIENCE = {}
          }
          if (membersStatsPrivate.Items[masIndex].hasOwnProperty("DESIGN")) {
            membersStatsPrivate.Items[masIndex].DESIGN = JSON.parse(membersStatsPrivate.Items[masIndex].DESIGN)
          } else {
            membersStatsPrivate.Items[masIndex].DESIGN = {}
          }
          if (membersStatsPrivate.Items[masIndex].hasOwnProperty("DEVELOP")) {
            membersStatsPrivate.Items[masIndex].DEVELOP = JSON.parse(membersStatsPrivate.Items[masIndex].DEVELOP)
          } else {
            membersStatsPrivate.Items[masIndex].DEVELOP = {}
          }
          if (membersStatsPrivate.Items[masIndex].hasOwnProperty("COPILOT")) {
            membersStatsPrivate.Items[masIndex].COPILOT = JSON.parse(membersStatsPrivate.Items[masIndex].COPILOT)
          } else {
            membersStatsPrivate.Items[masIndex].COPILOT = {}
          }
          esData.push({ index: { _index: esMemberStatsPrivateIndices, _type: esMemberStatsPrivateMappings, _id: setPrivateId } })
          esData.push(util.cleanse(membersStatsPrivate.Items[masIndex]))
          
          util.add(userIdsCompleted, setPrivateId)
          loader.display(loader.MESSAGES.ONLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          console.log(loader.MESSAGES.SKIP + " --> setPrivateId == " +  setPrivateId)
          loader.display(loader.MESSAGES.SKIP, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if(esData.length > 0) {
        let esResponse = await myElasticSearch.bulkToIndex(elasticClient, esData, false, 1)
        if (esResponse.errors == false) {
          loader.display(loader.MESSAGES.NEXT, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.ESOFFLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      
      if (membersStatsPrivate.LastEvaluatedKey) {
        statsLastEvaluatedKeyArray[segment].key = membersStatsPrivate.LastEvaluatedKeys;
        statsLastEvaluatedKeyArray[segment].count = countStats[segment];
        
        scanMemberStatsPrivate(membersStatsPrivate.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, totalItemCount)
      } else {
        loader.display(loader.MESSAGES.COMPLETED, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
        checkStats[segment] = true;
        if (checkStats.every(util.isTrue)) {
          startTime = moment().format("DD-MM-YYYY HH:mm:ss");
          util.durationTaken("Write to file (" + fullFilePath + ") - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
          fs.writeFile(fullFilePath, JSON.stringify(userIdsCompleted), function (err) {
            if (err) {
              return console.log(err);
            }
            util.durationTaken("Write to file (" + fullFilePath + ") - End -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
          });
        }
      }
    }
  } catch (err) {
    loader.display(loader.MESSAGES.DBOFFLINE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
    setTimeout(function () {
      loader.display(loader.MESSAGES.REVOKE, statsLastEvaluatedKeyArray, totalItemCount, countStats.reduce(function (a, b) { return a + b; }, 0), Number((((countStats.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)), Number(((countStats[segment] / (totalItemCount / totalSegments)) * 100).toFixed(1)), segment, totalSegments, startTime, colorScheme)
      scanMemberStatsPrivate(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, totalItemCount)
    }, 5000);
  }
}

async function cleanUp() {
  return new Promise(function (resolve, reject) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Giving 10 sec to 'Cancel' the Cleanup");
    setTimeout(function () {
      //myElasticSearch.dropIndex(elasticClient, esMemberStatsIndices);
      console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Droped Index Done");
      setTimeout(function () {
        //myElasticSearch.createESProfileTraitIndex(elasticClient, esMemberStatsIndices);
        console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Create Index Done");
        setTimeout(function () {
          //myElasticSearch.createESProfileMapping(elasticClient, esMemberStatsIndices, esMemberStatsMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Done");
          //myElasticSearch.createESProfileTraitMapping(elasticClient, esMemberStatsIndices, esMemberTraitsMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Traits Done");
          resolve(true);
        }, 2000);
      }, 2000);
    }, 10000);
  });
}

async function getMemberStatsPublic(esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < statsLastEvaluatedKeyArray.length; i++) {
      countStats[i] = 0;
      checkStats[i] = false;
      scanMemberStatsPublic(statsLastEvaluatedKeyArray[i].key, i, statsLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, totalItemCount)
    }
    resolve(true);
  });
}

async function getMemberStatsPrivate(esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < statsLastEvaluatedKeyArray.length; i++) {
      countStats[i] = 0;
      checkStats[i] = false;
      scanMemberStatsPrivate(statsLastEvaluatedKeyArray[i].key, i, statsLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, totalItemCount)
    }
    resolve(true);
  });
}

async function kickStart(args) {
  startTime = moment().format("DD-MM-YYYY HH:mm:ss");

  if (args.indexOf("cleanup") > -1) {
    util.durationTaken("Clean Up - Start  -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
    if (await cleanUp()) {
      util.durationTaken("Clean Up - End  -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
    }
  }

  if (args.indexOf("dev") > -1) {
    if (args.indexOf("public") > -1) {
      fullFilePath = "./userid-completed/stats-dev.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Stats Migration - Dev - Public - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberStats', statsLastEvaluatedKeyArray)
      await getMemberStatsPublic(esConfig.dev.esMemberStatsIndices, esConfig.dev.esMemberStatsMappings, fullFilePath, totalItemCount)
    } else if (args.indexOf("private") > -1) {
      fullFilePath = "./userid-completed/stats-dev.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Stats Migration - Dev - Private - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberStats_Private', statsLastEvaluatedKeyArray)
      await getMemberStatsPrivate(esConfig.dev.esMemberStatsIndices, esConfig.dev.esMemberStatsMappings, fullFilePath, totalItemCount)
    }
  } else if (args.indexOf("prod") > -1) {
    if (args.indexOf("public") > -1) {
      fullFilePath = "./userid-completed/stats-prod.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Stats Migration - Prod - Public - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberStats', statsLastEvaluatedKeyArray)
      await getMemberStatsPublic(esConfig.prod.esMemberStatsIndices, esConfig.prod.esMemberStatsMappings, fullFilePath, totalItemCount)
    } else if (args.indexOf("private") > -1) {
      fullFilePath = "./userid-completed/stats-prod.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Stats Migration - Prod - Private - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberStats_Private', statsLastEvaluatedKeyArray)
      await getMemberStatsPrivate(esConfig.prod.esMemberStatsIndices, esConfig.prod.esMemberStatsMappings, fullFilePath, totalItemCount)
    }
  }
}

/*
    $ cd /Users/at397596/Documents/Workspace/Topcoder/tc-aws-es-migration-repository/tc-aws-es-migration

    Options - cleanup dev/prod public/private
    node members.stats.js cleanup
    node members.stats.js dev public
    node members.stats.js dev private
    node members.stats.js prod public
    node members.stats.js prod private
*/
kickStart(args);