const { HLTV } = require('hltv')
var matchType = require('hltv').MatchType
const fs = require('fs')

// Don't get banned from HLTV, be respectful, use rate-limiter-flexible later
function sleep(milliSeconds) {
  var startTime = new Date().getTime();
  while (new Date().getTime() < startTime + milliSeconds);
}

function gettestData(){
  // Search matches by date, LAN events only
  HLTV.getMatchesStats({startDate: '2019-11-02', endDate: '2019-11-02',
    matchType: matchType.LAN})

    // Write getMatchesStats
    // then grab MatchMapStats of an individual match to get the matchPage ID
    .then(res => {
      var MatchMapStatsid = res[0].id
      var to_write = JSON.stringify(res, null, 2)
      fs.writeFile('test_getMatchesStats.txt', to_write, (err) => console.log(err))

      sleep(1000)
      return HLTV.getMatchMapStats({id: MatchMapStatsid})
    })

    // Write MatchMapStats
    .then(res => {
      var to_write = JSON.stringify(res, null, 2)
      fs.writeFile('test_getMatchMapStats.txt', to_write, (err) => console.log(err))

      sleep(1000)
      var matchPageID = res.matchPageID
      return HLTV.getMatch({id: matchPageID})
    })

    // Write getMatch then exit
    .then(res => {
      var to_write = JSON.stringify(res, null, 2)
      fs.writeFile('test_getMatch.txt', to_write, err => console.log(err))
    })
}

gettestData()
