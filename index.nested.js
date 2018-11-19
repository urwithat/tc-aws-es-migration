var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var fs = require("fs");
var Promise = require('bluebird');
var moment = require('moment');
var styleme = require('styleme')
//styleme.extend()

var esConfig = JSON.parse(fs.readFileSync("aws-es-config.json"));

var elasticClient = new elasticsearch.Client({  
    host: esConfig.dev.esHost
    //log: 'trace'
});

AWS.config.loadFromPath('./aws-dev-config.json');

const dynamo = new AWS.DynamoDB.DocumentClient()

var count = [];
var limit = 1000;
var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme) {
    await dynamo.scan(memberProfileTraitsParams).promise().then(function(memberProfileTraits) {
        if (memberProfileTraits != null) {
            for (let mptIndex = 0; mptIndex < memberProfileTraits.Items.length; mptIndex++) {
                const memberProfileTrait = memberProfileTraits.Items[mptIndex];
                if(memberProfileTrait.hasOwnProperty("traits")) {
                    memberProfileTrait.traits = JSON.parse(memberProfileTrait.traits);
                }
                if(memberProfileTrait.hasOwnProperty("createdAt")) {
                    memberProfileTrait.createdAt = moment(memberProfileTrait.createdAt).valueOf();
                }
                if(memberProfileTrait.hasOwnProperty("updatedAt")) {
                    memberProfileTrait.updatedAt = moment(memberProfileTrait.updatedAt).valueOf();
                }
                addToIndex(userId + memberProfileTrait.traitId, memberProfileTrait, 
                            esConfig.dev.esIndex, esConfig.dev.esTraitType);
            }
            return true;
        }
    }).catch(function(err) {
        console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Profile Trait - Failed - Invoke again", colorScheme))
        scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme);
    });
}

async function getMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme) {
    return new Promise(function (resolve, reject) {
        resolve(scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme));
    });
}

async function scanMemberProfile(lastEvaluatedKey, segment, totalSegments, colorScheme) {
    const memberProfilesParams = {
        TableName: 'MemberProfile',
        Limit: limit,
        ExclusiveStartKey: lastEvaluatedKey,
        Segment: segment,
        TotalSegments: totalSegments
    }

    const memberProfilesParamsUser = {
        TableName : "MemberProfile",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames:{
            "#userId": "userId"
        },
        ExpressionAttributeValues: {
            ":userId": 40154303
        },
        ExclusiveStartKey: lastEvaluatedKey
    }

    //const memberProfiles = await dynamo.query(memberProfilesParamsUser).promise();
    const memberProfiles = await dynamo.scan(memberProfilesParams).promise();
    if (memberProfiles != null) {
        console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ")" + moment().format("DD-MM-YYYY hh:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
        for (let mpIndex = 0; mpIndex < memberProfiles.Items.length; mpIndex++) {
            const memberProfile = memberProfiles.Items[mpIndex];
            if(memberProfile.hasOwnProperty("addresses")) {
                memberProfile.addresses = JSON.parse(memberProfile.addresses);
            }

            count[segment] = count[segment] + 1;
            console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + count[segment] + "|" + limit + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Find Member Profile --> UserID == " + memberProfile.userId, colorScheme))
            addToIndex(memberProfile.userId, memberProfile, esConfig.dev.esIndex, esConfig.dev.esType);
            var memberProfileTraitsParams = {
                TableName: "MemberProfileTrait",
                FilterExpression: "#userId = :userId",
                ExpressionAttributeNames: {
                    "#userId": "userId"
                },
                ExpressionAttributeValues: {
                    ":userId": Number(memberProfile.userId)
                }
            };
            if(await getMemberProfileTraits(memberProfile.userId, memberProfileTraitsParams, segment, totalSegments, colorScheme)) {
                //console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Member details added to ES ------->> ")
            }
        }
        if(memberProfiles.LastEvaluatedKey) {
            scanMemberProfile(memberProfiles.LastEvaluatedKey, segment, totalSegments, colorScheme);
        }
    }
}

function createESProfileTraitIndex() {
    return elasticClient.indices.create({
        index: esConfig.dev.esIndex,
        mapping: {
            profile: {
                handle: {
                    type: 'string'
                },
                handleLower: {
                    type: 'string'
                },
                userId: {
                    type: 'long'
                }
            },
            profiletrait: {
                traitId: {
                    type: 'string'
                },
                userId: {
                    type: 'long'
                }
            }
        }
    });
}

function addToIndex(id, data, index, type) {
    return elasticClient.index({
        index: index,
        type: type,
        id: id,
        body: data
    });
}

function dropIndex() {
    return elasticClient.indices.delete({
        index: esConfig.dev.esIndex,
    });
}

function searchIndex() {
    elasticClient.search({
        index: esConfig.dev.esIndex,
        type: esConfig.dev.esType,
        body: {
            query: {
                multi_match: {
                    query: 'profile111111',
                    fields: ['_id']
                }
            }
        }
    })
    .then(res => console.log(JSON.stringify(res, null, 5)))
}

async function kickStart() {
    //dropIndex();
    //console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Droped Index Done");
    setTimeout(function() {
        //createESProfileTraitIndex();
        console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Migration");
        var total = 90;
        
        for(var i=0; i < total; i++) {
            count[i] = 0;
            scanMemberProfile(null, i, total, colorScheme[i%10])
        }
    }, 2000);
}

kickStart();