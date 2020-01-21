const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
var db = require('./models.js')

// Don't get banned from HLTV, be respectful, use rate-limiter-flexible later
function sleep (milliSeconds) {
  var startTime = new Date().getTime()
  while (new Date().getTime() < startTime + milliSeconds);
}

function gettestData () {
  // Search matches by date, LAN events only
  HLTV.getMatchesStats({
    startDate: '2019-11-03',
    endDate: '2019-11-03',
    matchType: matchType.LAN
  })

    // Write getMatchesStats
    // then grab MatchMapStats of an individual match to get the matchPage ID
    .then(res => {
      var MatchMapStatsid = res[0].id
      var to_write = JSON.stringify(res, null, 2)
      fs.writeFile('test_getMatchesStats.txt', to_write, (err) => console.log(err))

      sleep(1500)
      return HLTV.getMatchMapStats({ id: MatchMapStatsid })
    })

    // Write MatchMapStats
    .then(MatchMapStats => {
      var to_write = JSON.stringify(MatchMapStats, null, 2)
      fs.writeFile('test_getMatchMapStats.txt', to_write, (err) => console.log(err))

      sleep(1500)
      var matchPageID = MatchMapStats.matchPageID
      // Keep the round summary data in MatchMapStats to save to DB later
      return Promise.all([HLTV.getMatch({ id: matchPageID }), MatchMapStats])
    })

    // Write getMatch then exit
    .then(([res, MatchMapStats]) => {
      var to_write = JSON.stringify(res, null, 2)
      console.log('Still have the getMatchMapStats results?')
      console.log(MatchMapStats)
      fs.writeFile('test_getMatch.txt', to_write, err => console.log(err))
    })
}

gettestData()
var test_getMatchesStats = JSON.parse(fs.readFileSync('./test_getMatchesStats.txt', 'utf8'))
var test_getMatchMapStats = JSON.parse(fs.readFileSync('./test_getMatchMapStats.txt', 'utf8'))
var test_getMatch = JSON.parse(fs.readFileSync('./test_getMatch.txt'))
