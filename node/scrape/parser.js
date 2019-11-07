const fs = require('fs')
const { HLTV } = require('hltv')

var test_getMatchesStats = JSON.parse(fs.readFileSync('test_getMatchesStats.txt', 'utf8'))
var test_getMatchMapStats = JSON.parse(fs.readFileSync('test_getMatchMapStats.txt', 'utf8'))
var test_getMatch = JSON.parse(fs.readFileSync('test_getMatch.txt'))

console.log(test_getMatchesStats[1])
var date = new Date(test_getMatchesStats[1].date)
console.log(test_getMatchMapStats)
console.log(date)
console.log(test_getMatch)
var test_demos_arr = test_getMatch.demos 
var test_demo = test_demos_arr.filter(demo => demo.name === 'GOTV Demo')
console.log()

