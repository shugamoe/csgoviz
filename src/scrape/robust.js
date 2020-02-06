var forever = require('forever-monitor')

var child = new (forever.Monitor)('./index.js', {
  max: 3,
  silent: false,
  args: []
})

child.on('exit', function () {
  console.log('Exited after 3 restarts')
})

child.on('restart', function () {
  console.error('Forever restarting script for ' + child.times + ' time')
})

child.on('exit:code', function (code) {
  console.error('Forever detected script exited with code ' + code)
})

// child.on('stdout', function(thing) {
// console.log(thing)
// });
//
// child.on('stderr', function(thing) {
// console.error(`Err: ${thing}`)
// });

child.start()
