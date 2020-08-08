var moment = require('moment');
var styleme = require('styleme')

var loaderDisplay = [];

const MESSAGES = {
  ONLINE: 'Online',
  OFFLINE: 'Offline',
  NEXT: 'Next',
  SKIP: 'Skip',
  REVOKE: 'Revoke',
  COMPLETED: 'Completed'
}

var self = module.exports = {
  display: function (message, key, totalPercentage, segmentPercentage, segment, totalSegments, startTime, colorScheme) {
    if (loaderDisplay.length == 0) {
      var resultTemplate = " (----------) <..> <..>"
      for(var box=0; box < totalSegments; box++) {
        loaderDisplay.push(resultTemplate)
      }
    }
    for (var point=1; point < 11; point++) {
      if (point <= segmentPercentage / 10) {
        loaderDisplay[segment] = self.replaceAt(loaderDisplay[segment], point + 1, "X")
      } else {
        loaderDisplay[segment] = self.replaceAt(loaderDisplay[segment], point + 1, "-")
      }
      loaderDisplay[segment] = self.replaceBetween(loaderDisplay[segment], (self.nthIndex(loaderDisplay[segment], "<", 1)), self.nthIndex(loaderDisplay[segment], ">", 1), message)
      loaderDisplay[segment] = self.replaceBetween(loaderDisplay[segment], (self.nthIndex(loaderDisplay[segment], "<", 2)), self.nthIndex(loaderDisplay[segment], ">", 2), key)
    }
    self.updateLoader(startTime, totalPercentage, colorScheme)
  },
  replaceAt: function (value, index, replacement) {
    return value.substr(0, index) + replacement + value.substr(index + replacement.length)
  },
  replaceBetween: function (value, startIndex, endIndex, replacement) {
    return value.substr(0, startIndex + 1) + replacement + value.substr(endIndex)
  },
  replaceAllComma: function (value, replace) {
    return value.replace(/,/g, "")
  },
  nthIndex: function (value, find, occurrence) {
    var L= value.length, i= -1;
    while(occurrence-- && i++<L){
        i= value.indexOf(find, i);
        if (i < 0) break;
    }
    return i;
  },
  updateLoader: function (startTime, totalPercentage, colorScheme) {
    var data = self.replaceAllComma(loaderDisplay.toString(), "")
    // console.log(styleme.style(" " + Number(totalPercentage) + "% " + data + " | " + self.estimatedDuration(startTime, totalPercentage) + " \r", colorScheme))
    process.stdout.write(styleme.style(" " + totalPercentage + "% " + data + " | " + self.estimatedDuration(startTime, totalPercentage) + " \r", colorScheme))
  },
  estimatedDuration: function (startTime, totalPercentage) {
    now = moment().format("DD-MM-YYYY HH:mm:ss")
    var diff = moment(now,"DD/MM/YYYY HH:mm:ss").diff(moment(startTime,"DD/MM/YYYY HH:mm:ss"))
    var secounds = moment.duration(diff).asSeconds()
    var remainingSec = ((secounds/totalPercentage)*(100-totalPercentage))
    var remainingDuration = moment.duration(remainingSec, 'seconds');
    if (Number.isNaN(remainingDuration.seconds())) {
      return "Calculating Estimated Completion Time"
    } else {
      return "day(" +  remainingDuration.days() + ") hour(" + remainingDuration.hours() + ") min(" + remainingDuration.minutes() + ") sec(" + remainingDuration.seconds() + ")"
    }
  },
  MESSAGES
};