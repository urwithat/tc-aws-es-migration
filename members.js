var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')
var Promise = require('bluebird');

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');
var loader = require('./reuse/loader');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
let dynamoDB;
var dynamoDBDocC;
var elasticClient;
var esMemberIndices;
var esMemberMappings;
var esMemberTraitsMappings;
if (args.indexOf("dev") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    // log: 'trace'
  });
  esMemberIndices = esConfig.dev.esMemberIndices
  esMemberMappings = esConfig.dev.esMemberMappings
  esMemberTraitsMappings = esConfig.dev.esMemberTraitsMappings
} else if (args.indexOf("prod") > -1) {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
  AWS.config.loadFromPath('./config/aws-prod-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.prod.esHost,
  });
  esMemberIndices = esConfig.prod.esMemberIndices
  esMemberMappings = esConfig.prod.esMemberMappings
  esMemberTraitsMappings = esConfig.prod.esMemberTraitsMappings
} else {
  console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
  AWS.config.loadFromPath('./config/aws-dev-config.json');
  dynamoDB = new AWS.DynamoDB();
  dynamoDBDocC = new AWS.DynamoDB.DocumentClient();
  elasticClient = new elasticsearch.Client({
    host: esConfig.dev.esHost,
    // log: 'trace'
  });
  esMemberIndices = esConfig.dev.esMemberIndices
  esMemberMappings = esConfig.dev.esMemberMappings
  esMemberTraitsMappings = esConfig.dev.esMemberTraitsMappings
}

var countProfileTrait = [];
var checkProfileTrait = [];

// Prod - (TT 00:00:00:30:588 / ETC 00:00:09:55:028)
// Dev  - (TT 00:00:00:04:774 / ETC 00:00:00:00:000)
var limitProfileTrait = 500;
var profileTraitLastEvaluatedKeyArray = [{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0}]

var countProfile = [];
var checkProfile = [];

// Prod - (TT 00:00:00:32:772 / ETC 00:00:37:06:029)
// Dev  - (TT 00:00:00:32:837 / ETC 00:00:18:23:671)
var limitProfile = 1000;
var profileLastEvaluatedKeyArray = [{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0},{key:null,count:0}]

var startTime;
var fullFilePath;
var userIdsCompleted;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberProfileTraits(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount) {
  const memberProfileTraitsParams = {
    TableName: 'MemberProfileTrait',
    Limit: limitProfileTrait,
    ExclusiveStartKey: lastEvaluatedKey,
    Segment: segment,
    TotalSegments: totalSegments,
    ConsistentRead: true,
  }

  try {
    const memberProfileTraits = await dynamoDBDocC.scan(memberProfileTraitsParams).promise();
    if (memberProfileTraits != null) {
      var esData = []
      for (let mptIndex = 0; mptIndex < memberProfileTraits.Items.length; mptIndex++) {
        const memberProfileTrait = memberProfileTraits.Items[mptIndex];
        countProfileTrait[segment] = countProfileTrait[segment] + 1;
        if (!util.exists(userIdsCompleted, memberProfileTrait.userId + memberProfileTrait.traitId)) {
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
          esData.push({ index: { _index: esMemberIndices, _type: esMemberTraitsMappings, _id: memberProfileTrait.userId + memberProfileTrait.traitId } })
          esData.push(util.cleanse(memberProfileTrait))
          util.add(userIdsCompleted, memberProfileTrait.userId + memberProfileTrait.traitId)
          loader.display(loader.MESSAGES.ONLINE, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.SKIP, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if (esData.length > 0) {
        let esResponse = await myElasticSearch.bulkToIndex(elasticClient, esData, false);
        if (esResponse.errors == false) {
          loader.display(loader.MESSAGES.ESUPDATED, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.ESOFFLINE, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if (memberProfileTraits.LastEvaluatedKey) {
        profileTraitLastEvaluatedKeyArray[segment].key = memberProfileTraits.LastEvaluatedKey;
        profileTraitLastEvaluatedKeyArray[segment].count = countProfileTrait[segment];
        scanMemberProfileTraits(memberProfileTraits.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount);
      } else {
        loader.display(loader.MESSAGES.COMPLETED, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        checkProfileTrait[segment] = true;
        if (checkProfileTrait.every(util.isTrue)) {
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
    loader.display(loader.MESSAGES.DBOFFLINE, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
    setTimeout(function () {
      loader.display(loader.MESSAGES.REVOKE, profileTraitLastEvaluatedKeyArray, totalItemCount, countProfileTrait.reduce(function (a, b) { return a + b; }, 0), Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfileTrait[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
      scanMemberProfileTraits(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount)
    }, 5000);
  }
}

async function scanMemberProfile(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, fullFilePath, totalItemCount) {
  const memberProfilesParams = {
    TableName: 'MemberProfile',
    Limit: limitProfile,
    ExclusiveStartKey: lastEvaluatedKey,
    Segment: segment,
    TotalSegments: totalSegments
  }
  try {
    const memberProfiles = await dynamoDBDocC.scan(memberProfilesParams).promise();
    if (memberProfiles != null) {
      var esData = []
      for (let mpIndex = 0; mpIndex < memberProfiles.Items.length; mpIndex++) {
        const memberProfile = memberProfiles.Items[mpIndex];
        countProfile[segment] = countProfile[segment] + 1;
        if (!util.exists(userIdsCompleted, memberProfile.userId)) {

          if (memberProfile.hasOwnProperty("addresses")) {
            memberProfile.addresses = JSON.parse(memberProfile.addresses);
          }
          if (memberProfile.hasOwnProperty("emailVerifyTokenDate")) {
            memberProfile.emailVerifyTokenDate = moment(memberProfile.emailVerifyTokenDate).valueOf();
          }
          if (memberProfile.hasOwnProperty("newEmailVerifyTokenDate")) {
            memberProfile.newEmailVerifyTokenDate = moment(memberProfile.newEmailVerifyTokenDate).valueOf();
          }
          if (memberProfile.hasOwnProperty("memberSince")) {
            memberProfile.memberSince = moment(memberProfile.memberSince).valueOf()
          }
          if (memberProfile.hasOwnProperty("updatedBy")) {
            memberProfile.updatedBy = memberProfile.userId
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

          esData.push({ index: { _index: esMemberIndices, _type: esMemberMappings, _id: memberProfile.userId } })
          esData.push(util.cleanse(memberProfile))
          
          util.add(userIdsCompleted, memberProfile.userId)
          loader.display(loader.MESSAGES.ONLINE, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.SKIP, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if(esData.length > 0) {
        let esResponse = await myElasticSearch.bulkToIndex(elasticClient, esData, false)
        if (esResponse.errors == false) {
          loader.display(loader.MESSAGES.ESUPDATED, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        } else {
          loader.display(loader.MESSAGES.ESOFFLINE, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        }
      }
      if (memberProfiles.LastEvaluatedKey) {
        profileLastEvaluatedKeyArray[segment].key = memberProfiles.LastEvaluatedKey;
        profileLastEvaluatedKeyArray[segment].count = countProfile[segment];
        scanMemberProfile(memberProfiles.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, fullFilePath, totalItemCount)
      } else {
        loader.display(loader.MESSAGES.COMPLETED, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
        checkProfile[segment] = true;
        if (checkProfile.every(util.isTrue)) {
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
    loader.display(loader.MESSAGES.DBOFFLINE, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
    setTimeout(function () {
      loader.display(loader.MESSAGES.REVOKE, profileLastEvaluatedKeyArray, totalItemCount, countProfile.reduce(function (a, b) { return a + b; }, 0), Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100))), Number(((countProfile[segment] / (totalItemCount / totalSegments)) * 100)), segment, totalSegments, startTime, colorScheme)
      scanMemberProfile(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, fullFilePath, totalItemCount)
    }, 5000);
  }
}

async function cleanUp() {
  return new Promise(function (resolve, reject) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Giving 10 sec to 'Cancel' the Cleanup");
    setTimeout(function () {
      //myElasticSearch.dropIndex(elasticClient, esMemberIndices);
      console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Droped Index Done");
      setTimeout(function () {
        //myElasticSearch.createESProfileTraitIndex(elasticClient, esMemberIndices);
        console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Create Index Done");
        setTimeout(function () {
          //myElasticSearch.createESProfileMapping(elasticClient, esMemberIndices, esMemberMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Done");
          //myElasticSearch.createESProfileTraitMapping(elasticClient, esMemberIndices, esMemberTraitsMappings);
          console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Mapping for Profile Traits Done");
          resolve(true);
        }, 2000);
      }, 2000);
    }, 10000);
  });
}

async function getMemberProfile(esMemberIndices, esMemberMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < profileLastEvaluatedKeyArray.length; i++) {
      countProfile[i] = 0;
      checkProfile[i] = false;
      scanMemberProfile(profileLastEvaluatedKeyArray[i].key, i, profileLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberIndices, esMemberMappings, fullFilePath, totalItemCount)
    }
    resolve(true);
  });
}

async function getMemberProfileTraits(esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < profileTraitLastEvaluatedKeyArray.length; i++) {
      countProfileTrait[i] = 0;
      checkProfileTrait[i] = false;
      scanMemberProfileTraits(profileTraitLastEvaluatedKeyArray[i].key, i, profileTraitLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount)
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

  if (args.indexOf("profile") > -1) {
    var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberProfile', profileLastEvaluatedKeyArray)
    if (args.indexOf("dev") > -1) {
      fullFilePath = "./userid-completed/profile-dev.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Profile Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      await getMemberProfile(esConfig.dev.esMemberIndices, esConfig.dev.esMemberMappings, fullFilePath, totalItemCount)
    } else if (args.indexOf("prod") > -1) {
      fullFilePath = "./userid-completed/profile-prod.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Profile Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      await getMemberProfile(esConfig.prod.esMemberIndices, esConfig.prod.esMemberMappings, fullFilePath, totalItemCount)
    }
  } else if (args.indexOf("profiletraits") > -1) {
    var totalItemCount = await util.findTotalItemCount(dynamoDB, 'MemberProfileTrait', profileTraitLastEvaluatedKeyArray)
    if (args.indexOf("dev") > -1) {
      fullFilePath = "./userid-completed/profile-trait-dev.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Profile Traits Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      await getMemberProfileTraits(esConfig.dev.esMemberIndices, esConfig.dev.esMemberTraitsMappings, fullFilePath, totalItemCount)
    } else if (args.indexOf("prod") > -1) {
      fullFilePath = "./userid-completed/profile-trait-prod.json"
      userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
      util.durationTaken("Profile Traits Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
      await getMemberProfileTraits(esConfig.prod.esMemberIndices, esConfig.prod.esMemberTraitsMappings, fullFilePath, totalItemCount)
    }
  }
}

/*
    Options - cleanup p/pt dev/prod
    node members.js cleanup
    node members.js profile dev
    node members.js profiletraits dev
    node members.js profile prod
    node members.js profiletraits prod
*/
kickStart(args);