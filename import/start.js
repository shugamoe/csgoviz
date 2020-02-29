var forever = require('forever-monitor')
require('console-stamp')(console, 'mmm/dd/yyyy | HH:MM.l')

var child = new (forever.Monitor)('./index.js', {
  max: 2,
  silent: false,
  // logFile: "./logs/",
  args: []
})

child.on('exit', function () {
  console.log('Exited after this.options restarts')
})

child.on('restart', function () {
  console.log('Forever restarting script for ' + child.times + ' time')
})

child.on('exit:code', function (code) {
  console.log('Forever detected script exited with code ' + code)
})

// child.on('stdout', function(thing) {
// console.log(thing)
// });
//
// child.on('stderr', function(thing) {
// console.error(`Err: ${thing}`)
// });

child.start()
