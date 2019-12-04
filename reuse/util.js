const cleanDeep = require('clean-deep');
var moment = require('moment');
var _ = require('lodash');

module.exports = {
    cleanse: function (obj) {
        return cleanDeep(obj, { 
            emptyArrays: false, 
            emptyObjects: false, 
            emptyStrings: false, 
            nullValues: true, 
            undefinedValues: true
        })
    },
    durationTaken: function (msg, start, end) {
        var ms = moment(end,"DD/MM/YYYY HH:mm:ss").diff(moment(start,"DD/MM/YYYY HH:mm:ss"));
        var d = moment.duration(ms);
        console.log(msg + "Took :: " + d.days() + " day(s), " +  d.hours() + " hour(s), " + d.minutes() + " mmin(s), " + d.seconds() + " sec(s)");
    },
    add: function (array, value) {
        if (array.indexOf(value) === -1) {
            array.push(value);
        }
    },
    exists: function (array, value) {
        if (array.indexOf(value) === -1) {
            return false
        } else {
            return true
        }
    },
    remove: function (array, value) {
        var index = array.indexOf(value);
        if (index !== -1) {
            array.splice(index, 1);
        }
    },
    isTrue:  function (currentValue) {
        return currentValue === true;
    },
    findTagById: function (data, id) {
        return _.find(data, { 'id': id });
    }
};