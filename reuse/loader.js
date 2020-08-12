var moment = require('moment');
var styleme = require('styleme')

var loaderDisplay = [];

const MESSAGES = {
  ONLINE:    ' Online  ',
  DBOFFLINE: 'dbOffline',
  ESOFFLINE: 'esOffline',
  ESUPDATED: 'esUpdated',
  SKIP:      '  Skip   ',
  REVOKE:    ' Revoke  ',
  COMPLETED: 'Completed',
  STARTED:   ' Started '
}

var self = module.exports = {
  display: function (message, key, totalItemCount, completedItemCount, totalPercentage, segmentPercentage, segment, totalSegments, startTime, colorScheme) {
    if (loaderDisplay.length == 0) {
      var resultTemplate = " (----------) <" + MESSAGES.STARTED + ">"
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
    }
    self.updateLoader(startTime, key, totalItemCount, completedItemCount, totalPercentage, colorScheme)
  },
  replaceAt: function (value, index, replacement) {
    return value.substr(0, index) + replacement + value.substr(index + replacement.length)
  },
  replaceBetween: function (value, startIndex, endIndex, replacement) {
    return value.substr(0, startIndex + 1) + replacement + value.substr(endIndex)
  },
  replaceAllComma: function (value, replace) {
    return value.replace(/,/g, replace)
  },
  replaceAllQuote: function (value, replace) {
    return value.replace(/"/g, replace)
  },
  nthIndex: function (value, find, occurrence) {
    var L= value.length, i= -1;
    while(occurrence-- && i++<L){
        i= value.indexOf(find, i);
        if (i < 0) break;
    }
    return i;
  },
  updateLoader: function (startTime, key, totalItemCount, completedItemCount, totalPercentage, colorScheme) {
    var data = self.replaceAllComma(loaderDisplay.toString(), "")
    console.log(styleme.style(" " + ((Number(totalPercentage).toFixed(1)).toString()).padEnd(3) + "% " + " | " + totalItemCount + "/" + (completedItemCount).toString().padEnd(totalItemCount.toString().length) + " | " + data + " | " + self.estimatedDuration(startTime, totalPercentage) + " | " + JSON.stringify(key) + " \r", colorScheme))
    // process.stdout.write(styleme.style(" " + ((Number(totalPercentage).toFixed(1)).toString()).padEnd(3) + "% " + " | " + totalItemCount + "/" + (completedItemCount).toString().padEnd(totalItemCount.toString().length) + " | " + data + " | " + self.estimatedDuration(startTime, totalPercentage) + " | " + key + " \r", colorScheme))
  },
  estimatedDuration: function (startTime, totalPercentage) {
    var now = moment().format("DD-MM-YYYY HH:mm:ss:SSS")
    var diff = moment(now,"DD/MM/YYYY HH:mm:ss:SSS").diff(moment(startTime,"DD/MM/YYYY HH:mm:ss:SSS"))
    var timeTaken = moment.duration(diff)
    var estTimeCompletion = ((timeTaken.asSeconds()/totalPercentage)*(100-totalPercentage))
    var etc = moment.duration(estTimeCompletion, 'seconds');

    let estimatedFormat = "(TT " + self.zeroPad(timeTaken.days(), 2) + ":" + self.zeroPad(timeTaken.hours(), 2) + ":" + self.zeroPad(timeTaken.minutes(), 2) + ":" + self.zeroPad(timeTaken.seconds(), 2) + ":" + self.zeroPad(Math.trunc(timeTaken.milliseconds()), 3)

    estimatedFormat += " / ETC " + self.zeroPad(etc.days(), 2) + ":" + self.zeroPad(etc.hours(), 2) + ":" + self.zeroPad(etc.minutes(), 2) + ":" + self.zeroPad(etc.seconds(), 2) + ":" + self.zeroPad(Math.trunc(etc.milliseconds()), 3) + ")"

    return (estimatedFormat).toString().padEnd(42)
  },
  zeroPad: function (num, places) {
    var zero = places - num.toString().length + 1;
    return Array(+(zero > 0 && zero)).join("0") + num;
  },
  MESSAGES
};