var Promise = require('bluebird');

var obj = {
  createESProfileMapping: function (elasticClient, index, mapping) {
    return elasticClient.indices.putMapping({
      index: index,
      type: mapping,
      body: {
        properties: {
          handle: {
            type: 'string',
            fields: {
              phrase: {
                type: 'string',
                analyzer: 'ngram_analyzer',
                search_analyzer: 'standard'
              }
            },
            analyzer: 'keyword_analyzer'
          },
          handleSuggest: {
            type: "completion",
            analyzer: "simple",
            payloads: true,
            preserve_separators: true,
            preserve_position_increments: true,
            max_input_length: 50
          },
          handleLower: {
            type: 'string'
          },
          userId: {
            type: 'long'
          }
        }
      }
    });
  },

  createESProfileTraitMapping: function (elasticClient, index, mapping) {
    return elasticClient.indices.putMapping({
      index: index,
      type: mapping,
      body: {
        properties: {
          categoryName: {
            type: 'string'
          },
          traitId: {
            type: 'string'
          },
          userId: {
            type: 'long'
          }
        }
      }
    });
  },

  createESProfileTraitIndex: function (elasticClient, index) {
    return elasticClient.indices.create({
      index: index,
      body:
      {
        settings: {
          index: {
            analysis: {
              analyzer: {
                keyword_analyzer: {
                  filter: ["lowercase"],
                  type: "custom",
                  tokenizer: "keyword"
                },
                ngram_analyzer: {
                  filter: ["lowercase"],
                  type: "custom",
                  tokenizer: "my_ngram_tokenizer"
                }
              },
              tokenizer: {
                my_ngram_tokenizer: {
                  type: "nGram",
                  min_gram: "3",
                  max_gram: "20"
                }
              }
            }
          }
        },
        mapping: {
          profile: {
            properties: {
              handle: {
                type: 'string',
                fields: {
                  phrase: {
                    type: 'string',
                    analyzer: 'ngram_analyzer',
                    search_analyzer: 'standard'
                  }
                },
                analyzer: 'keyword_analyzer'
              },
              handleSuggest: {
                type: "completion",
                analyzer: "simple",
                payloads: true,
                preserve_separators: true,
                preserve_position_increments: true,
                max_input_length: 50
              },
              handleLower: {
                type: 'string'
              },
              userId: {
                type: 'long'
              }
            }
          },
          profiletrait: {
            properties: {
              categoryName: {
                type: 'string'
              },
              traitId: {
                type: 'string'
              },
              userId: {
                type: 'long'
              }
            }
          }
        }
      }
    });
  },

  addToIndex: async function (elasticClient, id, data, index, type) {
    try {
      const response = await elasticClient.index({
        index: index,
        type: type,
        id: id,
        body: data
      });
      return response
    } catch (err) {
      if (err.message.indexOf('mapper_parsing_exception') > -1) {
        console.log("Elastic Search Error :: Handle Manually :: " + (err.message) ? err.message : "Empty message" + " :: path - " + (err.path) ? err.path : "Empty path")
        return { errors: false }
      } else if (err.message.indexOf('Request Timeout') > -1) {
        console.log("Elastic Search Error :: Request Timeout :: Rerun :: " + (err.message) ? err.message : "Empty message" + " :: path - " + (err.path) ? err.path : "Empty path")
        setTimeout(function () {
          obj.addToIndex(elasticClient, id, data, index, type)
        }, 5000);
        return { errors: false }
      } else {
        console.log("Elastic Search Error :: Kill process")
        console.log(err);
        return process.abort();
      }
    }
  },

  bulkToIndex: async function (elasticClient, data, rerun, count) {
    return new Promise(async function (resolve, reject) {
      var check = await obj.ping(elasticClient, 1)
      console.log("ES Health check :: " + check + ", count :: " + count)
      var response
      try {
        if (rerun) {
          console.log("ES Bulk Index :: Rerun :: count :: " + count)
          count += 1
        } else {
          count = 1
        }
        response = await elasticClient.bulk({
          body: data,
          refresh: true
        });
        resolve(response)
      } catch (err) {
        if (err.message.indexOf('mapper_parsing_exception') > -1) {
          var msg = "ES Bulk Index :: Error :: Handle Manually :: " + (err.message) ? err.message : "Empty message" + " :: path - " + (err.path) ? err.path : "Empty path"
          response = { errors: false, msg }
          resolve(response)
        } else if (err.message.indexOf('Request Timeout') > -1) {
          var msg = "ES Bulk Index :: Error :: Request Timeout :: Rerun :: " + count + " :: MSG :: " + (err.message) ? err.message : "Empty message" + " :: path - " + (err.path) ? err.path : "Empty path"
          response = await obj.bulkToIndex(elasticClient, data, true, count)
          resolve(response)
        } else if (err.message.indexOf('No Living connections') > -1) {
          var msg = "ES Bulk Index :: Error :: No Living connections :: Rerun :: " + count + " :: MSG :: " + (err.message) ? err.message : "Empty message" + " :: path - " + (err.path) ? err.path : "Empty path"
          response = await obj.bulkToIndex(elasticClient, data, true, count)
          resolve(response)
        } else {
          console.log("ES Bulk Index :: Error :: Kill process")
          console.log(err);
          return process.abort();
        }
      }
    })
  },

  ping: async function (elasticClient, count) {
    return new Promise(async function (resolve, reject) {
      var check
      try {
        check = await elasticClient.ping({ requestTimeout: 10000 })
        resolve(check)
      } catch (ex) {
        console.log("ES Health check :: Wait for stabilization of server :: " + count)
        await obj.sleep(10000)
        resolve(await obj.ping(elasticClient, count + 1))
      }
    })
  },

  sleep: async function (ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  },

  dropIndex: function (elasticClient, index) {
    return elasticClient.indices.delete({
      index: index,
    });
  },

  searchDeleteIndex: function (userId, elasticClient, index, type) {
    elasticClient.deleteByQuery({
      index: index,
      type: type,
      body: {
        query: {
          multi_match: {
            query: userId,
            fields: ['userId']
          }
        }
      }
    }, function (err, res) {
      console.log('The elements deleted are: %s', JSON.stringify(res.elements, null, 5));
    })
  },

  searchIndex: function (userId, elasticClient, index, type) {
    elasticClient.search({
      index: index,
      type: type,
      body: {
        query: {
          multi_match: {
            query: userId,
            fields: ['userId']
          }
        }
      }
    })
      .then(res => console.log(JSON.stringify(res.hits.hits, null, 5)))
  },

  searchWildcard: function (elasticClient, index, type, term) {
    elasticClient.search({
      index: index,
      type: type,
      body: {
        query: {
          wildcard: {
            "_uid": term
          }
        }
      }
    })
      .then(res => console.log(JSON.stringify(res, null, 5)))
  }
};

module.exports = obj;