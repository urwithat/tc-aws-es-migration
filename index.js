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

var countProfileTrait = [];
var totalProfileTrait = 4;
var limitProfileTrait = 50;

var countProfile = [];
var totalProfile = 2;
var limitProfile = 100;

var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberProfileTraits(lastEvaluatedKey, segment, totalSegments, colorScheme) {
    const memberProfileTraitsParams = {
        TableName: 'MemberProfileTrait',
        Limit: limitProfileTrait,
        ExclusiveStartKey: lastEvaluatedKey,
        Segment: segment,
        TotalSegments: totalSegments
    }

    const memberProfilesTraitsParamsUser = {
        TableName: "MemberProfileTrait",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames:{
            "#userId": "userId"
        },
        ExpressionAttributeValues: {
            ":userId": 40154303
        },
        ExclusiveStartKey: lastEvaluatedKey
    }

    //await dynamo.query(memberProfilesTraitsParamsUser).promise().then(function(memberProfileTraits) {
    await dynamo.scan(memberProfileTraitsParams).promise().then(function(memberProfileTraits) {
        if (memberProfileTraits != null) {
            console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ")" + moment().format("DD-MM-YYYY hh:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
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
                countProfileTrait[segment] = countProfileTrait[segment] + 1;
                addToIndex(memberProfileTrait.userId + memberProfileTrait.traitId, memberProfileTrait, esConfig.dev.esIndex, esConfig.dev.esTraitType);
                console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countProfileTrait[segment] + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Find Member Profile --> UserID == " + memberProfileTrait.userId, colorScheme))
            }
            if(memberProfileTraits.LastEvaluatedKey) {
                scanMemberProfileTraits(memberProfileTraits.LastEvaluatedKey, segment, totalSegments, colorScheme);
            }
        }
    }).catch(function(err) {
        console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Profile Trait - Failed - Invoke again", colorScheme))
        scanMemberProfileTraits(lastEvaluatedKey, segment, totalSegments, colorScheme)
    });
}

async function scanMemberProfile(lastEvaluatedKey, segment, totalSegments, colorScheme) {
    const memberProfilesParams = {
        TableName: 'MemberProfile',
        Limit: limitProfile,
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

    //await dynamo.query(memberProfilesParamsUser).promise().then(function(memberProfiles) {
    await dynamo.scan(memberProfilesParams).promise().then(function(memberProfiles) {
        if (memberProfiles != null) {
            console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ")" + moment().format("DD-MM-YYYY hh:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
            for (let mpIndex = 0; mpIndex < memberProfiles.Items.length; mpIndex++) {
                const memberProfile = memberProfiles.Items[mpIndex];
                if(memberProfile.hasOwnProperty("addresses")) {
                    memberProfile.addresses = JSON.parse(memberProfile.addresses);
                }
                countProfile[segment] = countProfile[segment] + 1;
                addToIndex(memberProfile.userId, memberProfile, esConfig.dev.esIndex, esConfig.dev.esType);
                console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") (" + countProfile[segment] + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Find Member Profile --> UserID == " + memberProfile.userId, colorScheme))
            }
            if(memberProfiles.LastEvaluatedKey) {
                scanMemberProfile(memberProfiles.LastEvaluatedKey, segment, totalSegments, colorScheme);
            }
        }
    }).catch(function(err) {
        console.log(styleme.style("(" + segment + "|" + (totalSegments-1) + ") -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Profile - Failed - Invoke again", colorScheme))
        scanMemberProfile(lastEvaluatedKey, segment, totalSegments, colorScheme)
    });
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
    }, function callback(err, response, status){
        if(err) {
            addToIndex(id, data, index, type)
        }
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
    dropIndex();
    console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Droped Index Done");
    setTimeout(function() {
        createESProfileTraitIndex();
        console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Migration");
        
        for(var i=0; i < totalProfileTrait; i++) {
            countProfileTrait[i] = 0;
            scanMemberProfileTraits(null, i, totalProfileTrait, colorScheme[i % colorScheme.length])
        }
        
        for(var i=0; i < totalProfile; i++) {
            countProfile[i] = 0;
            scanMemberProfile(null, i, totalProfile, colorScheme[i % colorScheme.length])
        }
        
    }, 2000);
}

kickStart();