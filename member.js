var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
// var elasticDeleteQuery = require('elastic-deletebyquery');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
var dynamo;
var elasticClient;
var elasticSIndex;
var elasticSType;
var elasticSTraitType;
if (args.indexOf("dev") > -1) {
  console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Dev - Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamo = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    //log: 'trace'
  });
  elasticSIndex = esConfig.dev.esMemberIndices
  elasticSType = esConfig.dev.esMemberMappings
  elasticSTraitType = esConfig.dev.esMemberTraitsMappings
} else if (args.indexOf("prod") > -1) {
  console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Prod - Configuration");
  AWS.config.loadFromPath('./config/aws-prod-config.json');
  dynamo = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.prod.esHost,
    //log: 'trace'
  });
  elasticSIndex = esConfig.prod.esMemberIndices
  elasticSType = esConfig.prod.esMemberMappings
  elasticSTraitType = esConfig.prod.esMemberTraitsMappings
} else {
  console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Dev - Default Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamo = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    //log: 'trace'
  });
  elasticSIndex = esConfig.dev.esMemberIndices
  elasticSType = esConfig.dev.esMemberMappings
  elasticSTraitType = esConfig.dev.esMemberTraitsMappings
}
// elasticDeleteQuery(elasticClient);

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
  try {
    const memberProfileTraits = await dynamo.query(memberProfileTraitsParams).promise();
    if (memberProfileTraits != null) {
      for (let mptIndex = 0; mptIndex < memberProfileTraits.Items.length; mptIndex++) {
        const memberProfileTrait = memberProfileTraits.Items[mptIndex];
        if (memberProfileTrait.hasOwnProperty("traits")) {
          memberProfileTrait.traits = JSON.parse(memberProfileTrait.traits);
        }
        if (memberProfileTrait.hasOwnProperty("createdAt")) {
          memberProfileTrait.createdAt = moment(memberProfileTrait.createdAt).valueOf();
        }
        if (memberProfileTrait.hasOwnProperty("updatedAt")) {
          memberProfileTrait.updatedAt = moment(memberProfileTrait.updatedAt).valueOf();
        }
        if (memberProfileTrait.hasOwnProperty('traits')) {
          if (memberProfileTrait.traits.hasOwnProperty('data')) {
            memberProfileTrait.traits.data.forEach(function (item) {
              if (item.hasOwnProperty('birthDate')) {
                if (item.birthDate != null) {
                  item.birthDate = moment(item.birthDate).valueOf()
                } else {
                  item.birthDate = moment().valueOf()
                }
              }
              if (item.hasOwnProperty('memberSince')) {
                if (item.memberSince != null) {
                  item.memberSince = moment(item.memberSince).valueOf()
                } else {
                  item.memberSince = moment().valueOf()
                }
              }
              if (item.hasOwnProperty('timePeriodFrom')) {
                if (item.timePeriodFrom != null) {
                  item.timePeriodFrom = moment(item.timePeriodFrom).valueOf()
                } else {
                  item.timePeriodFrom = moment().valueOf()
                }
              }
              if (item.hasOwnProperty('timePeriodTo')) {
                if (item.timePeriodTo != null) {
                  item.timePeriodTo = moment(item.timePeriodTo).valueOf()
                } else {
                  item.timePeriodTo = moment().valueOf()
                }
              }
            });
          }
        }
        let esResponse = await myElasticSearch.addToIndex(elasticClient, userId + memberProfileTrait.traitId, util.cleanse(memberProfileTrait), esMemberIndices, esMemberTraitsMappings);
        if (esResponse) {
          console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Found Member Profile Trait --> UserID == " + memberProfileTrait.userId + ", TraitId == " + memberProfileTrait.traitId, colorScheme))
        } else {
          console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Error Member Profile Trait --> UserID == " + memberProfileTrait.userId + ", TraitId == " + memberProfileTrait.traitId, colorScheme))
        }
      }
      return true;
    }
  } catch (err) {
    console.log(err)
    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Profile Trait - Failed - Invoke again", colorScheme))
  }
}

async function getMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
  return new Promise(function (resolve, reject) {
    resolve(scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings));
  });
}

async function scanMemberProfile(userId, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
  const memberProfilesParamsUser = {
    TableName: "MemberProfile",
    KeyConditionExpression: "#userId = :userId",
    ExpressionAttributeNames: {
      "#userId": "userId"
    },
    ExpressionAttributeValues: {
      ":userId": Number(userId)
    }
  }

  const memberProfiles = await dynamo.query(memberProfilesParamsUser).promise();
  if (memberProfiles != null) {
    for (let mpIndex = 0; mpIndex < memberProfiles.Items.length; mpIndex++) {
      const memberProfile = memberProfiles.Items[mpIndex];
      if (memberProfile.hasOwnProperty("addresses")) {
        memberProfile.addresses = JSON.parse(memberProfile.addresses);
      }
      if (memberProfile.hasOwnProperty("emailVerifyTokenDate")) {
        //memberProfile.emailVerifyTokenDate = moment(memberProfile.emailVerifyTokenDate).format("YYYY-mm-DD'T'hh:mm:ss.SSS'Z'")
        memberProfile.emailVerifyTokenDate = moment(memberProfile.emailVerifyTokenDate).valueOf()
      }
      if (memberProfile.hasOwnProperty("newEmailVerifyTokenDate")) {
        memberProfile.newEmailVerifyTokenDate = moment(memberProfile.newEmailVerifyTokenDate).valueOf()
      }
      if (memberProfile.hasOwnProperty("memberSince")) {
        memberProfile.memberSince = moment(memberProfile.memberSince).valueOf()
      }
      if (memberProfile.hasOwnProperty("updatedBy")) {
        memberProfile.updatedBy = memberProfile.userIdf
      }
      if (memberProfile.hasOwnProperty("createdBy")) {
        memberProfile.createdBy = memberProfile.userId
      }
      if (memberProfile.hasOwnProperty("createdAt")) {
        memberProfile.createdAt = moment(memberProfile.createdAt).valueOf();
      }
      if (memberProfile.hasOwnProperty("updatedAt")) {
        memberProfile.updatedAt = moment(memberProfile.updatedAt).valueOf();
      }

      // Adding Handle - Suggest
      memberProfile.handleSuggest = {
        input: memberProfile.handle,
        output: memberProfile.handle,
        payload: {
          handle: memberProfile.handle,
          userId: memberProfile.userId.toString(),
          id: memberProfile.userId.toString(),
          photoURL: memberProfile.photoURL,
          firstName: memberProfile.firstName,
          lastName: memberProfile.lastName,
        }
      }
      
      let esResponse = await myElasticSearch.addToIndex(elasticClient, memberProfile.userId, util.cleanse(memberProfile), esMemberIndices, esMemberMappings);
      if (esResponse) {
        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Found Member Profile --> UserID == " + memberProfile.userId + ", handleLower == " + memberProfile.handleLower, colorScheme))
      } else {
        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Error Member Profile --> UserID == " + memberProfile.userId + ", handleLower == " + memberProfile.handleLower, colorScheme))
      }

      var memberProfileTraitsParams = {
        TableName: "MemberProfileTrait",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames: {
          "#userId": "userId"
        },
        ExpressionAttributeValues: {
          ":userId": Number(memberProfile.userId)
        }
      };

      if (await getMemberProfileTraits(memberProfile.userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings)) {
        console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Member details added to ES ------->> ")
      }
    }
  }
}

// cleanup - delete all records in ES for user in profile traits
async function cleanupMemberProfileTraits(userId, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
  //myElasticSearch.searchDeleteIndex(userId, elasticClient, esMemberIndices, esMemberTraitsMappings);
}

async function kickStart(args) {
  if (args.indexOf("u") > -1) {
    if (args.indexOf("dev") > -1) {
      console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Dev Migration - " + args[2]);
      scanMemberProfile(args[2], 0, 1, colorScheme[0 % colorScheme.length], esConfig.dev.esMemberIndices, esConfig.dev.esMemberMappings, esConfig.dev.esMemberTraitsMappings)
    } else if (args.indexOf("prod") > -1) {
      console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Prod Migration - " + args[2]);
      scanMemberProfile(args[2], 0, 1, colorScheme[0 % colorScheme.length], esConfig.prod.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.prod.esMemberTraitsMappings)
    }
  } else if (args.indexOf("del") > -1) {
    if (args.indexOf("dev") > -1) {
      console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Dev Cleanup - " + args[2]);
      cleanupMemberProfileTraits(args[2], colorScheme[0 % colorScheme.length], esConfig.dev.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.dev.esMemberTraitsMappings)
    } else if (args.indexOf("prod") > -1) {
      console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Prod Cleanup - " + args[2]);
      cleanupMemberProfileTraits(args[2], colorScheme[0 % colorScheme.length], esConfig.prod.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.prod.esMemberTraitsMappings)
    }
  }
}

/*
    Options - del u dev/prod
    node member.js u dev 40154303
    node member.js u prod 40672021

    node member.js del dev 40154303
    node member.js del prod 40154303
*/
kickStart(args);