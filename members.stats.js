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
var dynamo;
var elasticClient;
var esMemberStatsIndices;
var esMemberStatsMappings;
if (args.indexOf("dev") > -1) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
    AWS.config.loadFromPath('./config/aws-dev-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
    elasticClient = new elasticsearch.Client({  
        host: esConfig.dev.esHost,
        //log: 'trace'
    });
    esMemberStatsIndices = esConfig.dev.esMemberStatsIndices
    esMemberStatsMappings = esConfig.dev.esMemberStatsMappings
} else if (args.indexOf("prod") > -1) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
    AWS.config.loadFromPath('./config/aws-prod-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
    elasticClient = new elasticsearch.Client({  
        host: esConfig.prod.esHost,
        //log: 'trace'
    });
    esMemberStatsIndices = esConfig.prod.esMemberStatsIndices
    esMemberStatsMappings = esConfig.prod.esMemberStatsMappings
} else {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
    AWS.config.loadFromPath('./config/aws-dev-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
    elasticClient = new elasticsearch.Client({  
        host: esConfig.dev.esHost,
        //log: 'trace'
    });
    esMemberStatsIndices = esConfig.dev.esMemberStatsIndices
    esMemberStatsMappings = esConfig.dev.esMemberStatsMappings
}

var countStats = [];
var checkStats = [];
var limitStats = 100;
//var statsLastEvaluatedKeyArray = [{"userId":22629283}]
var statsLastEvaluatedKeyArray = [null,null,null,null]

var startTime;
var fullFilePath;
var userIdsCompleted;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberStats(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsIndices, esMemberStatsMappings, fullFilePath) {
    const memberAggregatedStatsParams = {
        TableName: 'MemberAggregatedStats',
        Limit: limitStats,
        ExclusiveStartKey: lastEvaluatedKey,
        Segment: segment,
        TotalSegments: totalSegments
    }
    
    try {
        const membersAggregatedStats = await dynamo.scan(memberAggregatedStatsParams).promise();
        if (membersAggregatedStats != null) {
            console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))

            for (let masIndex = 0; masIndex < membersAggregatedStats.Items.length; masIndex++) {

                //var memberAggregatedStats = membersAggregatedStats.Items[masIndex];
                countStats[segment] = countStats[segment] + 1;
                if (!util.exists(userIdsCompleted, membersAggregatedStats.Items[masIndex].userId)) {

                    // console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " memberAggregatedStats.userId :: " + membersAggregatedStats.Items[masIndex].userId, colorScheme))

                    const membersEnteredStatsParams = {
                        TableName : "MemberEnteredStats",
                        KeyConditionExpression: "#userId = :userId",
                        ExpressionAttributeNames:{
                            "#userId": "userId"
                        },
                        ExpressionAttributeValues: {
                            ":userId": Number(membersAggregatedStats.Items[masIndex].userId)
                        }
                    }

                    const membersEnteredStats = await dynamo.query(membersEnteredStatsParams).promise();
                    if (membersEnteredStats != null && membersEnteredStats.Items.length > 0) {
                        var memberAggregatedStats = JSON.parse(membersAggregatedStats.Items[masIndex].stats)
                        var memberEnteredStats = JSON.parse(membersEnteredStats.Items[0].stats)
                        for(var attributename in memberEnteredStats) {
                            if (!memberAggregatedStats.hasOwnProperty(attributename)) {
                                memberAggregatedStats[attributename] = {}
                                memberAggregatedStats[attributename].score = 1.0
                                memberAggregatedStats[attributename].sources = ["USER_ENTERED"]
                                memberAggregatedStats[attributename].hidden = memberEnteredStats[attributename].hidden
                            } else {
                                if(!memberAggregatedStats[attributename].sources.includes("USER_ENTERED")) {
                                    memberAggregatedStats[attributename].sources.push("USER_ENTERED")
                                }
                            }
                        }
                        membersAggregatedStats.Items[masIndex].stats = memberAggregatedStats
                        membersAggregatedStats.Items[masIndex].handleLower = membersEnteredStats.Items[0].handleLower
                        membersAggregatedStats.Items[masIndex].userHandle = membersEnteredStats.Items[0].userHandle
                        membersAggregatedStats.Items[masIndex].updatedBy = membersEnteredStats.Items[0].userId
                        membersAggregatedStats.Items[masIndex].updatedAt = moment().valueOf()
                        // console.log(membersAggregatedStats.Items[masIndex])
                    } else {
                        const memberProfileParams = {
                            TableName : "MemberProfile",
                            KeyConditionExpression: "#userId = :userId",
                            ExpressionAttributeNames:{
                                "#userId": "userId"
                            },
                            ExpressionAttributeValues: {
                                ":userId": Number(membersAggregatedStats.Items[masIndex].userId)
                            }
                        }

                        const memberProfile = await dynamo.query(memberProfileParams).promise();
                        if (memberProfile != null && memberProfile.Items.length > 0) {
                            membersAggregatedStats.Items[masIndex].stats = JSON.parse(membersAggregatedStats.Items[masIndex].stats)
                            membersAggregatedStats.Items[masIndex].handleLower = memberProfile.Items[0].handleLower
                            membersAggregatedStats.Items[masIndex].userHandle = memberProfile.Items[0].handle
                            membersAggregatedStats.Items[masIndex].updatedBy = memberProfile.Items[0].userId
                            membersAggregatedStats.Items[masIndex].updatedAt = moment().valueOf()
                            // console.log(membersAggregatedStats.Items[masIndex])
                        }
                    }

                    // console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + membersAggregatedStats.Items[masIndex].stats, colorScheme))
                    
                    myElasticSearch.addToIndex(elasticClient, membersAggregatedStats.Items[masIndex].userId, util.cleanse(membersAggregatedStats.Items[masIndex]), esMemberStatsIndices, esMemberStatsMappings);

                    util.add(userIdsCompleted, membersAggregatedStats.Items[masIndex].userId)

                    console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countStats[segment] + "|" + countStats.reduce(function(a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Found Member Stats --> UserID == " + membersAggregatedStats.Items[masIndex].userId, colorScheme))
                } else {
                    console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countStats[segment] + "|" + countStats.reduce(function(a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile --> UserID == " + membersAggregatedStats.Items[masIndex].userId, colorScheme))
                }
            }
            if(membersAggregatedStats.LastEvaluatedKey) {
                statsLastEvaluatedKeyArray[segment] = JSON.stringify(membersAggregatedStats.LastEvaluatedKey);
                console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countStats[segment] + "|" + countStats.reduce(function(a, b) { return a + b; }, 0) + ") -->>" + " Last Evaluated Key :: " + statsLastEvaluatedKeyArray, colorScheme))
                scanMemberStats(membersAggregatedStats.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsIndices, esMemberStatsMappings, fullFilePath)
            } else {
                util.durationTaken("Completed - (" + segment + "|" + (totalSegments-1) + ") (" + countStats[segment] + "|" + countStats.reduce(function(a, b) { return a + b; }, 0) + ") -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
                checkStats[segment] = true;
                if (checkStats.every(util.isTrue)) {
                    startTime = moment().format("DD-MM-YYYY HH:mm:ss");
                    util.durationTaken("Write to file (" + fullFilePath + ") - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
                    fs.writeFile(fullFilePath, JSON.stringify( userIdsCompleted ), function(err) {
                        if(err) {
                            return console.log(err);
                        }
                        util.durationTaken("Write to file (" + fullFilePath + ") - End -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
                    }); 
                }
            }
        }
    } catch(err) {
        console.log(err);
        console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Profile - Failed - Invoke again", colorScheme))
        scanMemberStats(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberStatsIndices, esMemberStatsMappings, fullFilePath)
    }
}

async function cleanUp() {
    return new Promise(function (resolve, reject) {
        console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Giving 10 sec to 'Cancel' the Cleanup");
        setTimeout(function() {
            //myElasticSearch.dropIndex(elasticClient, esMemberStatsIndices);
            console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Droped Index Done");
            setTimeout(function() {
                //myElasticSearch.createESProfileTraitIndex(elasticClient, esMemberStatsIndices);
                console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Create Index Done");
                setTimeout(function() {
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


async function getMemberStats(esMemberStatsIndices, esMemberStatsMappings, fullFilePath) {
    return new Promise(function (resolve, reject) {
        for(var i=0; i < statsLastEvaluatedKeyArray.length; i++) {
            countStats[i] = 0;
            checkStats[i] = false;
            scanMemberStats(statsLastEvaluatedKeyArray[i], i, statsLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberStatsIndices, esMemberStatsMappings, fullFilePath)
        }
        resolve(true);
    });
}

async function kickStart(args) {
    startTime = moment().format("DD-MM-YYYY HH:mm:ss");

    if(args.indexOf("cleanup") > -1) {
        util.durationTaken("Clean Up - Start  -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        if(await cleanUp()) {
            util.durationTaken("Clean Up - End  -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        }
    }

    if(args.indexOf("dev") > -1) {
        fullFilePath = "./userid-completed/stats-dev.json"
        userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
        util.durationTaken("Stats Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        await getMemberStats(esConfig.dev.esMemberStatsIndices, esConfig.dev.esMemberStatsMappings, fullFilePath)
    } else if (args.indexOf("prod") > -1) {
        fullFilePath = "./userid-completed/stats-prod.json"
        userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
        util.durationTaken("Stats Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        await getMemberStats(esConfig.prod.esMemberStatsIndices, esConfig.prod.esMemberStatsMappings, fullFilePath)
    }
    
}

/*
    Options - cleanup dev/prod public/private
    node members.stats.js cleanup
    node members.stats.js dev public
    node members.stats.js dev private
    node members.stats.js prod
*/
kickStart(args);