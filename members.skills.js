var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')
var Promise = require('bluebird');
var request = require('request');

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');
var loader = require('./reuse/loader');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
let dynamoDB;
var dynamoDBDocC;
var elasticClient;
var esMemberSkillsIndices;
var esMemberSkillsMappings;
var tcApiUrl;
if (args.indexOf("dev") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
  });
  esMemberSkillsIndices = esConfig.dev.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.dev.esMemberSkillsMappings
  tcApiUrl = esConfig.dev.tcApiUrl
} else if (args.indexOf("prod") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
  AWS.config.loadFromPath('./config/aws-prod-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.prod.esHost,
  });
  esMemberSkillsIndices = esConfig.prod.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.prod.esMemberSkillsMappings
  tcApiUrl = esConfig.prod.tcApiUrl
} else {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    //log: 'trace'
  });
  esMemberSkillsIndices = esConfig.dev.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.dev.esMemberSkillsMappings
  tcApiUrl = esConfig.dev.tcApiUrl
}

var countSkills = [];
var checkSkills = [];

// Prod - (TT 00:00:00:30:815 / ETC 00:06:55:11:340)
// Dev  - (TT 00:00:00:30:130 / ETC 00:02:30:30:409)
var limitSkills = 500;
var skillsLastEvaluatedKeyArray = [{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0}]

var startTime;
var fullFilePath;
var userIdsCompleted;
var allTags;
var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberSkills(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath, totalItemCount) {
  const memberAggregatedSkillsParams = {
    TableName: 'MemberAggregatedSkills',
    Limit: limitSkills,
    ExclusiveStartKey: lastEvaluatedKey,
    Segment: segment,
    TotalSegments: totalSegments
  }
  try {
    const membersAggregatedSkills = await dynamoDBDocC.scan(memberAggregatedSkillsParams).promise();
    if (membersAggregatedSkills != null) {
      var esData = []
      for (let masIndex = 0; masIndex < membersAggregatedSkills.Items.length; masIndex++) {
        var memberAggregatedSkills
        try {
          memberAggregatedSkills = JSON.parse(membersAggregatedSkills.Items[masIndex].skills)
        } catch (err) {
          memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
        }
        for (var attributename in memberAggregatedSkills) {
          if (((memberAggregatedSkills[attributename].sources).indexOf("CHALLENGE") === -1)) {
            delete memberAggregatedSkills[attributename]
          }
        }
        membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills

        countSkills[segment] = countSkills[segment] + 1;
        if (!util.exists(userIdsCompleted, membersAggregatedSkills.Items[masIndex].userId)) {
          const membersEnteredSkillsParams = {
            TableName: "MemberEnteredSkills",
            KeyConditionExpression: "#userId = :userId",
            ExpressionAttributeNames: {
              "#userId": "userId"
            },
            ExpressionAttributeValues: {
              ":userId": Number(membersAggregatedSkills.Items[masIndex].userId)
            }
          }
          const membersEnteredSkills = await dynamoDBDocC.query(membersEnteredSkillsParams).promise();
          if (membersEnteredSkills != null && membersEnteredSkills.Items.length > 0) {
            var memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
            var memberEnteredSkills = JSON.parse(membersEnteredSkills.Items[0].skills)
            for (var attributename in memberEnteredSkills) {
              if (!memberAggregatedSkills.hasOwnProperty(attributename)) {
                memberAggregatedSkills[attributename] = {}
                memberAggregatedSkills[attributename].score = 1.0
                memberAggregatedSkills[attributename].sources = ["USER_ENTERED"]
                memberAggregatedSkills[attributename].hidden = memberEnteredSkills[attributename].hidden
              } else {
                if (!memberAggregatedSkills[attributename].sources.includes("USER_ENTERED")) {
                  memberAggregatedSkills[attributename].sources.push("USER_ENTERED")
                }
              }
            }
            membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills
            membersAggregatedSkills.Items[masIndex].handleLower = membersEnteredSkills.Items[0].handleLower
            membersAggregatedSkills.Items[masIndex].userHandle = membersEnteredSkills.Items[0].userHandle
            membersAggregatedSkills.Items[masIndex].updatedBy = membersEnteredSkills.Items[0].userId
            membersAggregatedSkills.Items[masIndex].updatedAt = moment().valueOf()
          } else {
            const memberProfileParams = {
              TableName: "MemberProfile",
              KeyConditionExpression: "#userId = :userId",
              ExpressionAttributeNames: {
                "#userId": "userId"
              },
              ExpressionAttributeValues: {
                ":userId": Number(membersAggregatedSkills.Items[masIndex].userId)
              }
            }
            const memberProfile = await dynamoDBDocC.query(memberProfileParams).promise();
            if (memberProfile != null && memberProfile.Items.length > 0) {
              //membersAggregatedSkills.Items[masIndex].skills = membersAggregatedSkills.Items[masIndex].skills
              membersAggregatedSkills.Items[masIndex].handleLower = memberProfile.Items[0].handleLower
              membersAggregatedSkills.Items[masIndex].userHandle = memberProfile.Items[0].handle
              membersAggregatedSkills.Items[masIndex].updatedBy = memberProfile.Items[0].userId
              membersAggregatedSkills.Items[masIndex].updatedAt = moment().valueOf()
            }
          }

          var memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
          for (var attributename in memberAggregatedSkills) {
            var tagDetails = util.findTagById(allTags, Number(attributename))
            if (tagDetails) {
              memberAggregatedSkills[attributename].tagName = tagDetails.name
            } else {
              delete memberAggregatedSkills[attributename];
            }
          }
          membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills

          esData.push({ index: { _index: esMemberSkillsIndices, _type: esMemberSkillsMappings, _id: membersAggregatedSkills.Items[masIndex].userId } })
          esData.push(util.cleanse(membersAggregatedSkills.Items[masIndex]))

          util.add(userIdsCompleted, membersAggregatedSkills.Items[masIndex].userId)
          loader.display(loader.MESSAGES.ONLINE, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.SKIP, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if(esData.length > 0) {
        let esResponse = await myElasticSearch.bulkToIndex(elasticClient, esData, false);
        if (esResponse.errors == false) {
          loader.display(loader.MESSAGES.ESUPDATED, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.ESOFFLINE, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if (membersAggregatedSkills.LastEvaluatedKey) {
        skillsLastEvaluatedKeyArray[segment].key = membersAggregatedSkills.LastEvaluatedKey;
        skillsLastEvaluatedKeyArray[segment].count = countSkills[segment];
        scanMemberSkills(membersAggregatedSkills.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath, totalItemCount)
      } else {
        // skillsLastEvaluatedKeyArray[segment].count = countSkills[segment];
        loader.display(loader.MESSAGES.COMPLETED, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        checkSkills[segment] = true;
        if (checkSkills.every(util.isTrue)) {
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
    loader.display(loader.MESSAGES.DBOFFLINE, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
    setTimeout(function () {
      loader.display(loader.MESSAGES.REVOKE, skillsLastEvaluatedKeyArray, totalItemCount, countSkills.reduce(function (a, b) { return a + b; }, 0), Number((((countSkills.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countSkills[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)

      scanMemberSkills(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath, totalItemCount)
    }, 5000);
  }
}

async function cleanUp() {
  return new Promise(function (resolve, reject) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Giving 10 sec to 'Cancel' the Cleanup");
    setTimeout(function () {
      //myElasticSearch.dropIndex(elasticClient, esMemberSkillsIndices);
      console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Droped Index Done");
      setTimeout(function () {
        //myElasticSearch.createESProfileTraitIndex(elasticClient, esMemberSkillsIndices);
        console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Create Index Done");
        setTimeout(function () {
          //myElasticSearch.createESProfileMapping(elasticClient, esMemberSkillsIndices, esMemberSkillsMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Done");
          //myElasticSearch.createESProfileTraitMapping(elasticClient, esMemberSkillsIndices, esMemberTraitsMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Traits Done");
          resolve(true);
        }, 2000);
      }, 2000);
    }, 10000);
  });
}

async function getMemberSkills(esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < skillsLastEvaluatedKeyArray.length; i++) {
      countSkills[i] = 0;
      checkSkills[i] = false;
      if (skillsLastEvaluatedKeyArray[i] != "stop") {
        scanMemberSkills(skillsLastEvaluatedKeyArray[i].key, i, skillsLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath, totalItemCount)
      } else {
        console.log(styleme.style("(" + i + "|" + (skillsLastEvaluatedKeyArray.length - 1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " Will not process as requested.", colorScheme[i % colorScheme.length]))
      }
    }
    resolve(true);
  });
}

async function getAllTags() {
  return new Promise(function (resolve, reject) {
    request({ url: tcApiUrl + '/v3/tags/?filter=domain%3DSKILLS%26status%3DAPPROVED&limit=1000' },
      function (error, response, body) {
        if (error != null) {
          reject(error);
        }
        resolve(body);
      }
    );
  })
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
    fullFilePath = "./userid-completed/skills-dev.json"
    userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
    util.durationTaken("Skills Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))

    var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberAggregatedSkills', skillsLastEvaluatedKeyArray)
    console.log("totalItemCount :: " + totalItemCount)

    await getMemberSkills(esConfig.dev.esMemberSkillsIndices, esConfig.dev.esMemberSkillsMappings, fullFilePath, totalItemCount)
  } else if (args.indexOf("prod") > -1) {
    fullFilePath = "./userid-completed/skills-prod.json"
    userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
    util.durationTaken("Skills Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))

    var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberAggregatedSkills', skillsLastEvaluatedKeyArray)
    console.log("totalItemCount :: " + totalItemCount)

    await getMemberSkills(esConfig.prod.esMemberSkillsIndices, esConfig.prod.esMemberSkillsMappings, fullFilePath, totalItemCount)
  }
}

/*
    Options - cleanup dev/prod
    node members.skills.js cleanup
    node members.skills.js dev
    node members.skills.js prod
*/
getAllTags().then(function (data) {
  console.log("Got Skills / Tags Data");
  allTags = JSON.parse(data)
  allTags = allTags.result.content
  kickStart(args);
}).catch(function (error) {
  console.log("Error in getAllTags : " + error);
});