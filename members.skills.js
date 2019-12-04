var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var moment = require('moment');
var styleme = require('styleme')
var Promise = require('bluebird');
var request = require('request');
//styleme.extend()

var myElasticSearch = require('./reuse/my-elastic-search');
var util = require('./reuse/util');

var esConfig = JSON.parse(fs.readFileSync("./config/aws-es-config.json"));

var args = process.argv.slice(2)
var dynamo;
var elasticClient;
var esMemberSkillsIndices;
var esMemberSkillsMappings;
var tcApiUrl;
if (args.indexOf("dev") > -1) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Configuration");
    AWS.config.loadFromPath('./config/aws-dev-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
    elasticClient = new elasticsearch.Client({  
        host: esConfig.dev.esHost,
        //log: 'trace'
    });
    esMemberSkillsIndices = esConfig.dev.esMemberSkillsIndices
    esMemberSkillsMappings = esConfig.dev.esMemberSkillsMappings
    tcApiUrl = esConfig.dev.tcApiUrl
} else if (args.indexOf("prod") > -1) {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Prod - Configuration");
    AWS.config.loadFromPath('./config/aws-prod-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
    elasticClient = new elasticsearch.Client({  
        host: esConfig.prod.esHost,
        //log: 'trace'
    });
    esMemberSkillsIndices = esConfig.prod.esMemberSkillsIndices
    esMemberSkillsMappings = esConfig.prod.esMemberSkillsMappings
    tcApiUrl = esConfig.prod.tcApiUrl
} else {
    console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Dev - Default Configuration");
    AWS.config.loadFromPath('./config/aws-dev-config.json');
    dynamo = new AWS.DynamoDB.DocumentClient();
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
var limitSkills = 100;
// Dev Env      :: Completed - (16|17) (4129|72762) -->> Took :: 0 day(s), 1 hour(s), 42 mmin(s), 7 sec(s)
// New Dev Env  :: Completed - (3|19) (3769|72763) -->> Took :: 0 day(s), 1 hour(s), 30 mmin(s), 49 sec(s)
var skillsLastEvaluatedKeyArray = [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]
//var skillsLastEvaluatedKeyArray = ["stop","stop","stop","stop","stop",{"userId":23150462},"stop","stop","stop","stop","stop",{"userId":22747540}]

// Prod Env      :: Completed - (0|17) (17461|313677) -->> Took :: 0 day(s), 4 hour(s), 56 mmin(s), 43 sec(s)
// New Prod Env  :: 
// var skillsLastEvaluatedKeyArray = [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]

var startTime;
var fullFilePath;
var userIdsCompleted;
var allTags;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberSkills(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath) {
    const memberAggregatedSkillsParams = {
        TableName: 'MemberAggregatedSkills',
        Limit: limitSkills,
        ExclusiveStartKey: lastEvaluatedKey,
        Segment: segment,
        TotalSegments: totalSegments
    }
    
    try {
        const membersAggregatedSkills = await dynamo.scan(memberAggregatedSkillsParams).promise();
        if (membersAggregatedSkills != null) {
            console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))

            for (let masIndex = 0; masIndex < membersAggregatedSkills.Items.length; masIndex++) {

                // Cleanup Members Aggregated Skills, remove `USER_ENTERED`
                var memberAggregatedSkills
                try {
                    memberAggregatedSkills = JSON.parse(membersAggregatedSkills.Items[masIndex].skills)
                } catch (err) {
                    memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
                }
                for(var attributename in memberAggregatedSkills) {
                    if (((memberAggregatedSkills[attributename].sources).indexOf("CHALLENGE") === -1)) {
                        delete memberAggregatedSkills[attributename]
                    }
                }
                membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills

                countSkills[segment] = countSkills[segment] + 1;
                if (!util.exists(userIdsCompleted, membersAggregatedSkills.Items[masIndex].userId)) {
                    const membersEnteredSkillsParams = {
                        TableName : "MemberEnteredSkills",
                        KeyConditionExpression: "#userId = :userId",
                        ExpressionAttributeNames:{
                            "#userId": "userId"
                        },
                        ExpressionAttributeValues: {
                            ":userId": Number(membersAggregatedSkills.Items[masIndex].userId)
                        }
                    }

                    const membersEnteredSkills = await dynamo.query(membersEnteredSkillsParams).promise();
                    if (membersEnteredSkills != null && membersEnteredSkills.Items.length > 0) {
                        var memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
                        var memberEnteredSkills = JSON.parse(membersEnteredSkills.Items[0].skills)
                        for(var attributename in memberEnteredSkills) {
                            if (!memberAggregatedSkills.hasOwnProperty(attributename)) {
                                memberAggregatedSkills[attributename] = {}
                                memberAggregatedSkills[attributename].score = 1.0
                                memberAggregatedSkills[attributename].sources = ["USER_ENTERED"]
                                memberAggregatedSkills[attributename].hidden = memberEnteredSkills[attributename].hidden
                            } else {
                                if(!memberAggregatedSkills[attributename].sources.includes("USER_ENTERED")) {
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
                            TableName : "MemberProfile",
                            KeyConditionExpression: "#userId = :userId",
                            ExpressionAttributeNames:{
                                "#userId": "userId"
                            },
                            ExpressionAttributeValues: {
                                ":userId": Number(membersAggregatedSkills.Items[masIndex].userId)
                            }
                        }

                        const memberProfile = await dynamo.query(memberProfileParams).promise();
                        if (memberProfile != null && memberProfile.Items.length > 0) {
                            //membersAggregatedSkills.Items[masIndex].skills = membersAggregatedSkills.Items[masIndex].skills
                            membersAggregatedSkills.Items[masIndex].handleLower = memberProfile.Items[0].handleLower
                            membersAggregatedSkills.Items[masIndex].userHandle = memberProfile.Items[0].handle
                            membersAggregatedSkills.Items[masIndex].updatedBy = memberProfile.Items[0].userId
                            membersAggregatedSkills.Items[masIndex].updatedAt = moment().valueOf()
                        }
                    }

                    var memberAggregatedSkills = membersAggregatedSkills.Items[masIndex].skills
                    for(var attributename in memberAggregatedSkills) {
                        var tagDetails = util.findTagById( allTags, Number(attributename) )
                        if(tagDetails) {
                            memberAggregatedSkills[attributename].tagName = tagDetails.name
                        } else {
                            console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Removing Invalid Tag Name :: " + attributename +  " For UserId :: " + membersAggregatedSkills.Items[masIndex].userId, colorScheme))
                            delete memberAggregatedSkills[attributename];
                        }
                    }
                    membersAggregatedSkills.Items[masIndex].skills = memberAggregatedSkills

                    // console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + membersAggregatedSkills.Items[masIndex].skills, colorScheme))
                    
                    myElasticSearch.addToIndex(elasticClient, membersAggregatedSkills.Items[masIndex].userId, util.cleanse(membersAggregatedSkills.Items[masIndex]), esMemberSkillsIndices, esMemberSkillsMappings);

                    util.add(userIdsCompleted, membersAggregatedSkills.Items[masIndex].userId)

                    console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countSkills[segment] + "|" + countSkills.reduce(function(a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Found Member Skills --> UserID == " + membersAggregatedSkills.Items[masIndex].userId, colorScheme))
                } else {
                    console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countSkills[segment] + "|" + countSkills.reduce(function(a, b) { return a + b; }, 0) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " - Skiped Member Profile --> UserID == " + membersAggregatedSkills.Items[masIndex].userId, colorScheme))
                }
            }
            if(membersAggregatedSkills.LastEvaluatedKey) {
                skillsLastEvaluatedKeyArray[segment] = JSON.stringify(membersAggregatedSkills.LastEvaluatedKey);
                console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countSkills[segment] + "|" + countSkills.reduce(function(a, b) { return a + b; }, 0) + ") -->>" + " Last Evaluated Key :: " + skillsLastEvaluatedKeyArray, colorScheme))
                scanMemberSkills(membersAggregatedSkills.LastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath)
            } else {
                util.durationTaken("Completed - (" + segment + "|" + (totalSegments-1) + ") (" + countSkills[segment] + "|" + countSkills.reduce(function(a, b) { return a + b; }, 0) + ") -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
                checkSkills[segment] = true;
                if (checkSkills.every(util.isTrue)) {
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
        scanMemberSkills(lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath)
    }
}

async function cleanUp() {
    return new Promise(function (resolve, reject) {
        console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Giving 10 sec to 'Cancel' the Cleanup");
        setTimeout(function() {
            //myElasticSearch.dropIndex(elasticClient, esMemberSkillsIndices);
            console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Droped Index Done");
            setTimeout(function() {
                //myElasticSearch.createESProfileTraitIndex(elasticClient, esMemberSkillsIndices);
                console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Create Index Done");
                setTimeout(function() {
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


async function getMemberSkills(esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath) {
    return new Promise(function (resolve, reject) {
        for(var i=0; i < skillsLastEvaluatedKeyArray.length; i++) {
            countSkills[i] = 0;
            checkSkills[i] = false;
            if(skillsLastEvaluatedKeyArray[i] != "stop") {
                scanMemberSkills(skillsLastEvaluatedKeyArray[i], i, skillsLastEvaluatedKeyArray.length, colorScheme[i % colorScheme.length], esMemberSkillsIndices, esMemberSkillsMappings, fullFilePath)
            } else {
                console.log(styleme.style("(" + i + "|" + (skillsLastEvaluatedKeyArray.length-1) + ") -->> " + moment().format("DD-MM-YYYY HH:mm:ss") + " Will not process as requested.", colorScheme[i % colorScheme.length]))
            }
        }
        resolve(true);
    });
}

async function getAllTags() {
    return new Promise(function (resolve, reject) {
        request({ url : tcApiUrl + '/v3/tags/?filter=domain%3DSKILLS%26status%3DAPPROVED&limit=1000' },
            function (error, response, body) {
                if(error != null) {
                    reject(error);
                }
                resolve(body);
            }
        );
    })
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
        fullFilePath = "./userid-completed/skills-dev.json"
        userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
        util.durationTaken("Skills Migration - Dev - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        await getMemberSkills(esConfig.dev.esMemberSkillsIndices, esConfig.dev.esMemberSkillsMappings, fullFilePath)
    } else if (args.indexOf("prod") > -1) {
        fullFilePath = "./userid-completed/skills-prod.json"
        userIdsCompleted = JSON.parse(fs.readFileSync(fullFilePath));
        util.durationTaken("Skills Migration - Prod - Start -->> ", startTime, moment().format("DD-MM-YYYY HH:mm:ss"))
        await getMemberSkills(esConfig.prod.esMemberSkillsIndices, esConfig.prod.esMemberSkillsMappings, fullFilePath)
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