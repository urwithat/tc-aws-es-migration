var AWS = require("aws-sdk");
var elasticsearch = require('elasticsearch');
var elasticDeleteQuery = require('elastic-deletebyquery');
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
elasticDeleteQuery(elasticClient);

var count = [];
var limit = 1000;
var colorScheme = ["bla,bre", "red,bwh", "bla,bgr", "bla,bye", "whi,bbl", "blu,bwh", "whi,bre", "blu,bye", "whi,bgr", "bla,bwh"]

async function scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
    await dynamo.query(memberProfileTraitsParams).promise().then(function(memberProfileTraits) {
        // console.log(memberProfileTraits)
        if (memberProfileTraits != null) {
            //console.log("traits :: " + memberProfileTraits.Items.length)
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
                if (memberProfileTrait.hasOwnProperty('traits')) {
                    if (memberProfileTrait.traits.hasOwnProperty('data')) {
                        memberProfileTrait.traits.data.forEach(function(item) { 
                            if (item.hasOwnProperty('birthDate')) {
                                console.log("birthDate :: " + item.birthDate)
                                if(item.birthDate != null) {
                                    //item.birthDate = moment(item.birthDate).format('YYYY-mm-DD hh:mm:ss')
                                    item.birthDate = moment(item.birthDate).valueOf()
                                } else {
                                    //item.birthDate = moment().format('YYYY-mm-DD hh:mm:ss')
                                    item.birthDate = moment().valueOf()
                                }
                            }
                            if (item.hasOwnProperty('memberSince')) {
                                if(item.memberSince != null) {
                                    item.memberSince = moment(item.memberSince).valueOf()
                                } else {
                                    item.memberSince = moment().valueOf()
                                }
                            }
                            if (item.hasOwnProperty('timePeriodFrom')) {
                                if(item.timePeriodFrom != null) {
                                    item.timePeriodFrom = moment(item.timePeriodFrom).valueOf()
                                } else {
                                    item.timePeriodFrom = moment().valueOf()
                                }
                            }
                            if (item.hasOwnProperty('timePeriodTo')) {
                                if(item.timePeriodTo != null) {
                                    item.timePeriodTo = moment(item.timePeriodTo).valueOf()
                                } else {
                                    item.timePeriodTo = moment().valueOf()
                                }
                            }
                        });
                    }
                }
                myElasticSearch.addToIndex(elasticClient, userId + memberProfileTrait.traitId, util.cleanse(memberProfileTrait), esMemberIndices, esMemberTraitsMappings);
            }
            return true;
        }
    }).catch(function(err) {
        console.log(err)
        console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Profile Trait - Failed - Invoke again", colorScheme))
        scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings);
    });
}

async function getMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
    return new Promise(function (resolve, reject) {
        resolve(scanMemberProfileTraits(userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings));
    });
}

async function scanMemberProfile(userId, lastEvaluatedKey, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings) {
    const memberProfilesParamsUser = {
        TableName : "MemberProfile",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames:{
            "#userId": "userId"
        },
        ExpressionAttributeValues: {
            ":userId": Number(userId)
        },
        ExclusiveStartKey: lastEvaluatedKey
    }

    const memberProfiles = await dynamo.query(memberProfilesParamsUser).promise();
    //const memberProfiles = await dynamo.scan(memberProfilesParams).promise();
    if (memberProfiles != null) {
        console.log(styleme.style(moment().format("DD-MM-YYYY hh:mm:ss") + " - LastEvaluatedKey ------->> " + JSON.stringify(lastEvaluatedKey), colorScheme))
        for (let mpIndex = 0; mpIndex < memberProfiles.Items.length; mpIndex++) {
            const memberProfile = memberProfiles.Items[mpIndex];
            if(memberProfile.hasOwnProperty("addresses")) {
                memberProfile.addresses = JSON.parse(memberProfile.addresses);
            }
            if(memberProfile.hasOwnProperty("emailVerifyTokenDate")) {
                //memberProfile.emailVerifyTokenDate = moment(memberProfile.emailVerifyTokenDate).format("YYYY-mm-DD'T'hh:mm:ss.SSS'Z'")
                memberProfile.emailVerifyTokenDate = moment(memberProfile.emailVerifyTokenDate).valueOf()
            }
            if(memberProfile.hasOwnProperty("memberSince")) {
                memberProfile.memberSince = moment(memberProfile.memberSince).valueOf()
            }
            if(memberProfile.hasOwnProperty("updatedBy")) {
                memberProfile.updatedBy = memberProfile.userId
            }
            if(memberProfile.hasOwnProperty("createdBy")) {
                memberProfile.createdBy = memberProfile.userId
            }
            if(memberProfile.hasOwnProperty("createdAt")) {
                memberProfile.createdAt = moment(memberProfile.createdAt).valueOf();
            }
            if(memberProfile.hasOwnProperty("updatedAt")) {
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

            console.log(styleme.style(" -->> " + moment().format("DD-MM-YYYY hh:mm:ss") + " - Find Member Profile --> UserID == " + memberProfile.userId, colorScheme))
            
            myElasticSearch.addToIndex(elasticClient, memberProfile.userId, util.cleanse(memberProfile), esMemberIndices, esMemberMappings);
            
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
            
            if(await getMemberProfileTraits(memberProfile.userId, memberProfileTraitsParams, segment, totalSegments, colorScheme, esMemberIndices, esMemberMappings, esMemberTraitsMappings)) {
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
    if(args.indexOf("u") > -1) {
        if(args.indexOf("dev") > -1) {
            console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Dev Migration - " + args[2]);
            scanMemberProfile(args[2], null, 0, 1, colorScheme[0 % colorScheme.length], esConfig.dev.esMemberIndices, esConfig.dev.esMemberMappings, esConfig.dev.esMemberTraitsMappings)
        } else if(args.indexOf("prod") > -1) {
            console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Prod Migration - " + args[2]);
            scanMemberProfile(args[2], null, 0, 1, colorScheme[0 % colorScheme.length],esConfig.prod.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.prod.esMemberTraitsMappings)
        }
    } else if(args.indexOf("del") > -1) {
        if(args.indexOf("dev") > -1) {
            console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Dev Cleanup - " + args[2]);
            cleanupMemberProfileTraits(args[2], colorScheme[0 % colorScheme.length], esConfig.dev.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.dev.esMemberTraitsMappings)
        } else if(args.indexOf("prod") > -1) {
            console.log(moment().format("DD-MM-YYYY hh:mm:ss") + " - Start Prod Cleanup - " + args[2]);
            cleanupMemberProfileTraits(args[2], colorScheme[0 % colorScheme.length],esConfig.prod.esMemberIndices, esConfig.prod.esMemberMappings, esConfig.prod.esMemberTraitsMappings)
        }
    }
}

/*
    Options - del u dev/prod
    node member.js del dev 40154303
    node member.js del prod 40154303

    node member.js u dev 40154303
    node member.js u prod 40672021
*/
kickStart(args);