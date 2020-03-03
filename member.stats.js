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
        //log: 'trace'
    });
    esMemberStatsIndices = esConfig.dev.esMemberStatsIndices
    esMemberStatsMappings = esConfig.dev.esMemberStatsMappings
}

var countStats = [];
var checkStats = [];
var limitStats = 10;

var startTime;
var fullFilePath;
var userIdsCompleted;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function queryMemberStatsPublic(colorScheme, esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, userId) { 
    const memberStatsPublicParams = {
        TableName: "MemberStats",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames: {
          "#userId": "userId"
        },
        ExpressionAttributeValues: {
          ":userId": Number(userId)
        }
    }
    
    try {
        const membersStatsPublic = await dynamoDBDocC.query(memberStatsPublicParams).promise();
        if (membersStatsPublic != null) {

            for (let masIndex = 0; masIndex < membersStatsPublic.Items.length; masIndex++) {
                
                var setPublicId = membersStatsPublic.Items[masIndex].userId + "_10";
                if (!util.exists(userIdsCompleted, setPublicId)) {
                    
                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " userId :: " + membersStatsPublic.Items[masIndex].userId, colorScheme))

                    membersStatsPublic.Items[masIndex].groupId = 10
                    if(membersStatsPublic.Items[masIndex].hasOwnProperty("maxRating")) {
                        membersStatsPublic.Items[masIndex].maxRating = JSON.parse(membersStatsPublic.Items[masIndex].maxRating)
                    }
                    if(membersStatsPublic.Items[masIndex].hasOwnProperty("DATA_SCIENCE")) {
                        membersStatsPublic.Items[masIndex].DATA_SCIENCE = JSON.parse(membersStatsPublic.Items[masIndex].DATA_SCIENCE)
                    }
                    if(membersStatsPublic.Items[masIndex].hasOwnProperty("DESIGN")) {
                        membersStatsPublic.Items[masIndex].DESIGN = JSON.parse(membersStatsPublic.Items[masIndex].DESIGN)
                    }
                    if(membersStatsPublic.Items[masIndex].hasOwnProperty("DEVELOP")) {
                        membersStatsPublic.Items[masIndex].DEVELOP = JSON.parse(membersStatsPublic.Items[masIndex].DEVELOP)
                    }
                    if(membersStatsPublic.Items[masIndex].hasOwnProperty("COPILOT")) {
                        membersStatsPublic.Items[masIndex].COPILOT = JSON.parse(membersStatsPublic.Items[masIndex].COPILOT)
                    }
                    
                    // console.log(util.cleanse(membersStatsPublic.Items[masIndex]))

                    myElasticSearch.addToIndex(elasticClient, setPublicId, util.cleanse(membersStatsPublic.Items[masIndex]), esMemberStatsPublicIndices, esMemberStatsPublicMappings);

                    // util.add(userIdsCompleted, setPublicId)

                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Found Member Stats --> userGroupID == " + setPublicId, colorScheme))
                } else {
                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile --> userGroupID == " + setPublicId, colorScheme))
                }
            }
        }
    } catch(err) {
        console.log(err);
        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Profile - Failed - Invoke again", colorScheme))
        queryMemberStatsPublic(colorScheme, esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, userId)
    }
}

async function queryMemberStatsPrivate(colorScheme, esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, userId) {
    const memberStatsPrivateParams = {
        TableName: "MemberStats_Private",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames: {
          "#userId": "userId"
        },
        ExpressionAttributeValues: {
          ":userId": Number(userId)
        }
    }
    
    try {
        const membersStatsPrivate = await dynamoDBDocC.query(memberStatsPrivateParams).promise();
        if (membersStatsPrivate != null) {
            
            for (let masIndex = 0; masIndex < membersStatsPrivate.Items.length; masIndex++) {
                var setPrivateId = membersStatsPrivate.Items[masIndex].userId + "_" + membersStatsPrivate.Items[masIndex].groupId;
                if (!util.exists(userIdsCompleted, setPrivateId)) {
                    
                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " userId :: " + membersStatsPrivate.Items[masIndex].userId, colorScheme))

                    if(membersStatsPrivate.Items[masIndex].hasOwnProperty("maxRating")) {
                        membersStatsPrivate.Items[masIndex].maxRating = JSON.parse(membersStatsPrivate.Items[masIndex].maxRating)
                    } else {
                        membersStatsPrivate.Items[masIndex].maxRating = {}
                    }
                    if(membersStatsPrivate.Items[masIndex].hasOwnProperty("DATA_SCIENCE")) {
                        membersStatsPrivate.Items[masIndex].DATA_SCIENCE = JSON.parse(membersStatsPrivate.Items[masIndex].DATA_SCIENCE)
                    } else {
                        membersStatsPrivate.Items[masIndex].DATA_SCIENCE = {}
                    }
                    if(membersStatsPrivate.Items[masIndex].hasOwnProperty("DESIGN")) {
                        membersStatsPrivate.Items[masIndex].DESIGN = JSON.parse(membersStatsPrivate.Items[masIndex].DESIGN)
                    } else {
                        membersStatsPrivate.Items[masIndex].DESIGN = {}
                    }
                    if(membersStatsPrivate.Items[masIndex].hasOwnProperty("DEVELOP")) {
                        membersStatsPrivate.Items[masIndex].DEVELOP = JSON.parse(membersStatsPrivate.Items[masIndex].DEVELOP)
                    } else {
                        membersStatsPrivate.Items[masIndex].DEVELOP = {}
                    }
                    if(membersStatsPrivate.Items[masIndex].hasOwnProperty("COPILOT")) {
                        membersStatsPrivate.Items[masIndex].COPILOT = JSON.parse(membersStatsPrivate.Items[masIndex].COPILOT)
                    } else {
                        membersStatsPrivate.Items[masIndex].COPILOT = {}
                    }

                    // console.log(util.cleanse(membersStatsPrivate.Items[masIndex]))

                    myElasticSearch.addToIndex(elasticClient, setPrivateId, util.cleanse(membersStatsPrivate.Items[masIndex]), esMemberStatsPrivateIndices, esMemberStatsPrivateMappings);

                    // util.add(userIdsCompleted, setPrivateId)

                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Found Member Stats --> userGroupID == " + setPrivateId, colorScheme))
                } else {
                    console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile --> userGroupID == " + setPrivateId, colorScheme))
                }
            }
        }
    } catch(err) {
        console.log(err);
        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Profile - Failed - Invoke again", colorScheme))
        queryMemberStatsPrivate(colorScheme, esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, userId)
    }
}

async function getMemberStatsPublic(esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, userId) {
    return new Promise(function (resolve, reject) {
        queryMemberStatsPublic(colorScheme[3 % colorScheme.length], esMemberStatsPublicIndices, esMemberStatsPublicMappings, fullFilePath, userId)
        resolve(true);
    });
}

async function getMemberStatsPrivate(esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, userId) {
    return new Promise(function (resolve, reject) {
        queryMemberStatsPrivate(colorScheme[2 % colorScheme.length], esMemberStatsPrivateIndices, esMemberStatsPrivateMappings, fullFilePath, userId)
        resolve(true);
    });
}

async function kickStart(args) {
    startTime = moment().format("DD-MM-YYYY HH:mm:ss");
    if ( typeof args[2] === 'undefined' ) {
        console.log("Please provide userId")
        return
    }

    if(args.indexOf("dev") > -1) {
        if(args.indexOf("public") > -1) {
            fullFilePath = "./userid-completed/stats-dev.json"
            userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
            util.durationTaken("Stats Migration - Dev - Public - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
            await getMemberStatsPublic(esConfig.dev.esMemberStatsIndices, esConfig.dev.esMemberStatsMappings, fullFilePath, args[2])
        } else if(args.indexOf("private") > -1) {
            fullFilePath = "./userid-completed/stats-dev.json"
            userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
            util.durationTaken("Stats Migration - Dev - Private - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
            await getMemberStatsPrivate(esConfig.dev.esMemberStatsIndices, esConfig.dev.esMemberStatsMappings, fullFilePath, args[2])
        }
    } else if (args.indexOf("prod") > -1) {
        if(args.indexOf("public") > -1) {
            fullFilePath = "./userid-completed/stats-prod.json"
            userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
            util.durationTaken("Stats Migration - Prod - Public - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
            await getMemberStatsPublic(esConfig.prod.esMemberStatsIndices, esConfig.prod.esMemberStatsMappings, fullFilePath, args[2])
        } else if(args.indexOf("private") > -1) {
            fullFilePath = "./userid-completed/stats-prod.json"
            userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
            util.durationTaken("Stats Migration - Prod - Private - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
            await getMemberStatsPrivate(esConfig.prod.esMemberStatsIndices, esConfig.prod.esMemberStatsMappings, fullFilePath, args[2])
        }
    }
    
}

/*
    $ cd /Users/at397596/Documents/Workspace/Topcoder/tc-aws-es-migration-repository/tc-aws-es-migration

    Options - dev/prod public/private userId
    node member.stats.js dev public 40153800
    node member.stats.js dev private 8547899
    node member.stats.js prod public 40154303
    node member.stats.js prod private 40154303
*/
kickStart(args);