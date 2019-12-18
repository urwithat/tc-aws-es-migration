var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')
var Promise = require('bluebird');
//styleme.extend()

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');

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
    //log: 'trace'
  });
  esMemberIndices = esConfig.dev.esMemberIndices
  esMemberMappings = esConfig.dev.esMemberMappings
  esMemberTraitsMappings = esConfig.dev.esMemberTraitsMappings
}

var countProfileTrait = [];
var checkProfileTrait = [];

// Prod - 
var limitProfileTrait = 3;
var profileTraitLastEvaluatedKeyArray = [null, null]

// Dev - Completed - (9|9) (109|689) -->> Took :: 0 day(s), 0 hour(s), 0 mmin(s), 5 sec(s)
// var limitProfileTrait = 20;
// var profileTraitLastEvaluatedKeyArray = [null, null, null, null, null, null, null, null, null, null]

var countProfile = [];
var checkProfile = [];

// Prod - 
var limitProfile = 3;
var profileLastEvaluatedKeyArray = [null, null]

// Dev - Completed - (5|9) (85218|846173) -->> Took :: 0 day(s), 2 hour(s), 0 mmin(s), 0 sec(s)
// var limitProfile = 10;
// var profileLastEvaluatedKeyArray = [null, null, null, null, null, null, null, null, null, null]

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
    TotalSegments: totalSegments
  }

  try {
    const memberProfileTraits = await dynamoDBDocC.scan(memberProfileTraitsParams).promise();
    if (memberProfileTraits != null) {
      console.log(styleme.style("(" + segment + "|" + (totalSegments - 1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
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
          myElasticSearch.addToIndex(elasticClient, memberProfileTrait.userId + memberProfileTrait.traitId, util.cleanse(memberProfileTrait), esMemberIndices, esMemberTraitsMappings);
          util.add(userIdsCompleted, memberProfileTrait.userId + memberProfileTrait.traitId)
          console.log(styleme.style("[" + Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfileTrait[segment] + "|" + countProfileTrait.reduce(function (a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Find Member Profile Traits --> UserID == " + memberProfileTrait.userId + memberProfileTrait.traitId, colorScheme))
        } else {
          console.log(styleme.style("[" + Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfileTrait[segment] + "|" + countProfileTrait.reduce(function (a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile Traits --> UserID == " + memberProfileTrait.userId + memberProfileTrait.traitId, colorScheme))
        }
      }
      if (memberProfileTraits.LastEvaluatedKey) {
        profileTraitLastEvaluatedKeyArray[segment] = JSON.stringify(memberProfileTraits.LastEvaluatedKey);
        console.log(styleme.style("[" + Number((((countProfileTrait.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfileTrait[segment] + "|" + countProfileTrait.reduce(function (a, b) { return a + b; }, 0) + ") -->>" + " Last Evaluated Key :: " + profileTraitLastEvaluatedKeyArray, colorScheme))
        scanMemberProfileTraits(memberProfileTraits.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount);
      } else {
        util.durationTaken("Completed - (" + segment + "|" + (totalSegments - 1) + ") (" + countProfileTrait[segment] + "|" + countProfileTrait.reduce(function (a, b) { return a + b; }, 0) + ") -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
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
    console.log(" Start :: Timeout =========================================================== ");
    setTimeout(function () {
      console.log(" End   :: Timeout =========================================================== ");
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
      console.log(styleme.style("(" + segment + "|" + (totalSegments - 1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
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
          myElasticSearch.addToIndex(elasticClient, memberProfile.userId, util.cleanse(memberProfile), esMemberIndices, esMemberMappings);

          util.add(userIdsCompleted, memberProfile.userId)
          console.log(styleme.style("[" + Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfile[segment] + "|" + countProfile.reduce(function (a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Find Member Profile --> UserID == " + memberProfile.userId, colorScheme))
        } else {
          console.log(styleme.style("[" + Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfile[segment] + "|" + countProfile.reduce(function (a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile --> UserID == " + memberProfile.userId, colorScheme))
        }
      }
      if (memberProfiles.LastEvaluatedKey) {
        profileLastEvaluatedKeyArray[segment] = JSON.stringify(memberProfiles.LastEvaluatedKey);
        console.log(styleme.style("[" + Number((((countProfile.reduce(function (a, b) { return a + b; }, 0) / totalItemCount) * 100)).toFixed(1)) + "%] (" + segment + "|" + (totalSegments - 1) + ") (" + countProfile[segment] + "|" + countProfile.reduce(function (a, b) { return a + b; }, 0) + ") -->>" + " Last Evaluated Key :: " + profileLastEvaluatedKeyArray, colorScheme))
        scanMemberProfile(memberProfiles.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, fullFilePath, totalItemCount)
      } else {
        util.durationTaken("Completed - (" + segment + "|" + (totalSegments - 1) + ") (" + countProfile[segment] + "|" + countProfile.reduce(function (a, b) { return a + b; }, 0) + ") -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
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
    console.log(" Start :: Timeout =========================================================== ");
    setTimeout(function () {
      console.log(" End   :: Timeout =========================================================== ");
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

async function getTotalItemCount(tableName) {
  try {
    const totalItemCountParams = {
      TableName: tableName
    }
    const totalItemCountData = await dynamoDB.describeTable(totalItemCountParams).promise()
    return totalItemCountData.Table.ItemCount
  } catch (ex) {
    return 0
  }
}

async function getMemberProfile(esMemberIndices, esMemberMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < profileLastEvaluatedKeyArray.length; i++) {
      countProfile[i] = 0;
      checkProfile[i] = false;
      scanMemberProfile(profileLastEvaluatedKeyArray[i], i, profileLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberIndices, esMemberMappings, fullFilePath, totalItemCount)
    }
    resolve(true);
  });
}


async function getMemberProfileTraits(esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount) {
  return new Promise(function (resolve, reject) {
    for (var i = 0; i < profileTraitLastEvaluatedKeyArray.length; i++) {
      countProfileTrait[i] = 0;
      checkProfileTrait[i] = false;
      scanMemberProfileTraits(profileTraitLastEvaluatedKeyArray[i], i, profileTraitLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberIndices, esMemberTraitsMappings, fullFilePath, totalItemCount)
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
    var totalItemCount = await getTotalItemCount('MemberProfile')
    console.log("totalItemCount :: " + totalItemCount)

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
    var totalItemCount = await getTotalItemCount('MemberProfileTrait')
    console.log("totalItemCount :: " + totalItemCount)

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