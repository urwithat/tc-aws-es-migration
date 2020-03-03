var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')
var Promise = require('bluebird');
var request = require('request');

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
var dynamoDBDocC;
var elasticClient;
var esMemberSkillsIndices;
var esMemberSkillsMappings;
var tcApiUrl;
if (args.indexOf("dev") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost
  });
  esMemberSkillsIndices = esConfig.dev.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.dev.esMemberSkillsMappings
  tcApiUrl = esConfig.dev.tcApiUrl
} else if (args.indexOf("prod") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
  AWS.config.loadFromPath('./config/aws-prod-config.json');
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.prod.esHost
  });
  esMemberSkillsIndices = esConfig.prod.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.prod.esMemberSkillsMappings
  tcApiUrl = esConfig.prod.tcApiUrl
} else {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    //log: 'trace'
  });
  esMemberSkillsIndices = esConfig.dev.esMemberSkillsIndices
  esMemberSkillsMappings = esConfig.dev.esMemberSkillsMappings
  tcApiUrl = esConfig.dev.tcApiUrl
}

var limitSkills = 10;
//var skillsLastEvaluatedKeyArray = [{"userId":22629283}]
var startTime;
var allTags;
var colorScheme = "bla,bre";

async function scanMemberSkills(userId, esMemberSkillsIndices, esMemberSkillsMappings) {
  const memberAggregatedSkillsParams = {
    TableName: "MemberAggregatedSkills",
    KeyConditionExpression: "#userId = :userId",
    ExpressionAttributeNames: {
      "#userId": "userId"
    },
    ExpressionAttributeValues: {
      ":userId": Number(userId)
    }
  }

  try {
    const membersAggregatedSkills = await dynamoDBDocC.query(memberAggregatedSkillsParams).promise();
    if (membersAggregatedSkills != null) {
      for (let masIndex = 0; masIndex < membersAggregatedSkills.Items.length; masIndex++) {

        // Cleanup Members Aggregated Skills, remove `USER_ENTERED`
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
          var memberAggregatedSkills1 = membersAggregatedSkills.Items[masIndex].skills
          var memberEnteredSkills = JSON.parse(membersEnteredSkills.Items[0].skills)
          for (var attributename in memberEnteredSkills) {
            if (!memberAggregatedSkills1.hasOwnProperty(attributename)) {
              memberAggregatedSkills1[attributename] = {}
              memberAggregatedSkills1[attributename].score = 1.0
              memberAggregatedSkills1[attributename].sources = ["USER_ENTERED"]
              memberAggregatedSkills1[attributename].hidden = memberEnteredSkills[attributename].hidden
            } else {
              if (!memberAggregatedSkills1[attributename].sources.includes("USER_ENTERED")) {
                memberAggregatedSkills1[attributename].sources.push("USER_ENTERED")
              }
            }
          }
          membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills1
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

        var memberAggregatedSkills2 = membersAggregatedSkills.Items[masIndex].skills
        for (var attributename in memberAggregatedSkills2) {
          var tagDetails = util.findTagById(allTags, Number(attributename))
          if (tagDetails) {
            memberAggregatedSkills2[attributename].tagName = tagDetails.name
          } else {
            console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Removing Invalid Tag Name :: " + attributename + " For UserId :: " + membersAggregatedSkills.Items[masIndex].userId, colorScheme))
            delete memberAggregatedSkills2[attributename];
          }
        }
        membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills2

        myElasticSearch.addToIndex(elasticClient, membersAggregatedSkills.Items[masIndex].userId, util.cleanse(membersAggregatedSkills.Items[masIndex]), esMemberSkillsIndices, esMemberSkillsMappings);

        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Found Member Skills --> UserID == " + membersAggregatedSkills.Items[masIndex].userId, colorScheme))
      }
    }
  } catch (err) {
    console.log(err);
  }
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

  if (args.indexOf("dev") > -1) {
    util.durationTaken("Skills Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
    await scanMemberSkills(args[1], esConfig.dev.esMemberSkillsIndices, esConfig.dev.esMemberSkillsMappings)
  } else if (args.indexOf("prod") > -1) {
    util.durationTaken("Skills Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
    await scanMemberSkills(args[1], esConfig.prod.esMemberSkillsIndices, esConfig.prod.esMemberSkillsMappings)
  }

}

/*
    Options - dev/prod
    
    node member.skills.js dev 40154303
    node member.skills.js prod 40154303
*/
getAllTags().then(function (data) {
  console.log("Got Skills / Tags Data");
  allTags = JSON.parse(data)
  allTags = allTags.result.content
  kickStart(args);
}).catch(function (error) {
  console.log("Error in getAllTags : " + error);
});