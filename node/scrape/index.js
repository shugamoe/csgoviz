const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
const MapDict = require('./maps.json')
const { extractArchive } = require('./utils.js')
const { importDemo, importMatch } = require('../import/index.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible');
var MatchSQL = require('../import/models.js').Match
// var pg = require('pg')
// var config = require('../import/config.json')
// var client = new pg.Client(config.connectionString)

const queryRLM = new RateLimiterMemory({
    points: 1,
    duration: 3,
});
const queryLimiter = new RateLimiterQueue(queryRLM, {
    maxQueueSize: 1000,
})


var orphanMapStats = []
var problemImports = []
var concurDL = 0
var curImport = ""
async function downloadDay(date_str){
  try {
    await queryLimiter.removeTokens(1)
    var matchesStats = await HLTV.getMatchesStats({
      startDate: date_str,
      endDate: date_str,
      matchType: matchType.LAN
    })
  } catch(err) {
    console.log("HLTV.getMatchesStats error")
    console.dir(err)
  }

  var matchMapStatsArr = []
  var matchArr = []
  // for (let i = 0; i < MatchesStats.length; i++){
  // for (let i = 0; i < 5; i++){
  matchesStats.forEach(async (matchStats) => { 
    try {
      await queryLimiter.removeTokens(1)
      var matchMapStats = await HLTV.getMatchMapStats({
        id: matchStats.id
      })
    } catch (err) {
      console.log("HLTV.getMatchMapStats error")
      console.dir(err)
    }

    try {
      queryLimiter.removeTokens(1)
      var match = await HLTV.getMatch({
        id: matchMapStats.matchPageID
      })
    } catch(err) {
      console.log("HLTV.getmatchMapStats error")
      console.dir(err)
    }

    // Download demo archive
    try {
      // Import the match data into SQL database, in case something goes wrong with the download or the import.
      do {
        // Snoozes function without pausing event loop
        await snooze(1000)
      }
      while (curImport)

      MatchSQL.findAll({
        attributes: ['match_id'],
        where: {match_id: match.id}
      }).then(async res => {
        if (res.length == 0){ // If we don't have that match_id in the DB already
          curImport = match.id
          await importMatch(match).then(curImport = "")
        } else {
          if (res[0].dataValues.match_id == match.id){
            orphanMapStats.push({json: matchMapStats, MapStatsID: matchStats.id})
            console.log(`Match ${match.id} is in the DB already.`)
            return null // If we have the match already, in the DB, don't bother trying to DL the demos.
          } else {
            console.log("Wut.")
          }
        }
      })

      do {
        // Snoozes function without pausing event loop
        // console.log("concurDL: %d", concurDL)
        await snooze(1000)
      }
      while (concurDL >= 2)

        downloadMatch(match, matchMapStats, matchStats.id).then(async fulfilled => {
          console.log(`Importing demos for Match: ${match.id}`)

          // for (let d=0; d < fulfilled.demos.length; d++) {
          fulfilled.demos.forEach(async (demo) => {
            // Is the current matchMapStats appropriate for the demo?
            var haveMapStats = demo.match(MapDict[matchMapStats.map])
            console.log(haveMapStats)
            var importmatchMapStats
            var importmatchMapStatsID
            if (haveMapStats) {
              importmatchMapStats = matchMapStats
              importmatchMapStatsID = matchStats.id
            } else { // If not, check orphans
              var matchingOrphans = orphanMapStats.filter(ms => {
                var sameMap = demo.match(MapDict[ms.json.map])
                return ((ms.matchPageID == match.id) && sameMap)
              })
              if (matchingOrphans.length == 1) {
                importmatchMapStats = matchingOrphans[0].json
                importmatchMapStatsID = matchingOrphans[0].MapStatsID
              } else {
                console.dir(matchingOrphans)
                console.dir(orphanMapStats)
                console.dir(fulfilled)
                throw new Error("No Orphan found?")
              }
            }
            do {
              // Snoozes function without pausing event loop
              await snooze(1000)
              // console.log(`Import for ${matchArr[i].id}-${fulfilled.demos[d]} waiting. . . curImport = ${curImport}`)
            }
            while (curImport)

            curImport = match.id + "|" + demo
            await importDemo(fulfilled.out_dir + demo, importmatchMapStats, importmatchMapStatsID, match).then(() => {
              curImport = ""
            }).catch((err) => {
              console.dir("Error importing demo")
              console.log(err)
              problemImports.push(match)
            })
          })
          // After importing all demos for the match, remove the orphan(s) used.
          orphanMapStats = orphanMapStats.filter(ms => ms.json.matchPageID != match.id)
        }).catch(() => '')
    } catch (err) {
      console.log(err)
    }
  })
}

setTimeout(function() {
}, 5000)

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms))


async function downloadMatch(Match, matchMapStats, matchMapStatsID){
  var demo_link = Match.demos.filter(demo => demo.name === 'GOTV Demo')[0].link
  demo_link = apiConfig.hltvUrl + demo_link

  var out_dir = '/home/jcm/matches/' + Match.id + '/'
  var out_path = out_dir + 'archive.rar'

  if (!fs.existsSync(out_dir)){
    fs.mkdirSync(out_dir, {recursive: true})
    console.log("%d folder created.", Match.id)
  }

  return new Promise((resolve, reject) => {
    concurDL += 1
    var out = fs.createWriteStream(out_path, {flags: 'wx'})
      .on('error', () => {
        orphanMapStats.push({json: matchMapStats, MapStatsID: matchMapStatsID})
        concurDL -= 1
        reject(`${Match.id} already downloaded or downloading (${out_path}), skipping. . .`)
      })
      .on('ready', () => {
        console.log("%d starting download. . .", Match.id)

        new FetchStream(demo_link)
          .pipe(out)
          .on('error', err => console.log(err, null)) // Could get 503 (others too possib.) log those for checking later?
          .on('finish', async () => {
            console.log("%d archive downloaded", Match.id)
            concurDL -= 1
            try {
              var demos = await extractArchive(out_path, out_dir)
            } catch(err) {
              console.dir(err)
            }
            resolve({out_dir: out_dir, 
              demos: demos ? demos : undefined
            }
            )
          })
      })
  })
}

var test_getMatchesStats = JSON.parse(fs.readFileSync('./test_getMatchesStats.txt', 'utf8'))
var test_getMatchMapStats = JSON.parse(fs.readFileSync('./test_getMatchMapStats.txt', 'utf8'))
var test_getMatch = JSON.parse(fs.readFileSync('./test_getMatch.txt'))
// downloadMatch(test_getMatch, test_getMatchMapStats)
downloadDay('2019-11-02')
