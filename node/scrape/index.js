const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
const config = require('./config.json')
const { extractArchive } = require('./utils.js')
const { importDemo } = require('../import/index.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible');

const queryRLM = new RateLimiterMemory({
    points: 1,
    duration: 3,
});
const queryLimiter = new RateLimiterQueue(queryRLM, {
    maxQueueSize: 2,
})


var orphanMapStats = []
var concurDL = 0
var curImport = ""
async function downloadDay(date_str){
  try {
    const remainingTokens = await queryLimiter.removeTokens(1)
    var MatchesStats = await HLTV.getMatchesStats({
      startDate: date_str,
      endDate: date_str,
      matchType: matchType.LAN
    })
  } catch(err) {
    console.log("HLTV.getMatchesStats error")
    console.dir(err)
  }

  var MatchMapStatsArr = []
  var MatchArr = []
  // for (let i = 0; i < MatchesStats.length; i++){
  for (let i = 0; i < 5; i++){
    try {
      await queryLimiter.removeTokens(1)
      MatchMapStatsArr[i] = await HLTV.getMatchMapStats({
        id: MatchesStats[i].id
      })
    } catch (err) {
      console.log("HLTV.getMatchMapStats error")
      console.dir(err)
    }

    try {
      await queryLimiter.removeTokens(1)
      MatchArr[i] = await HLTV.getMatch({
        id: MatchMapStatsArr[i].matchPageID
      })
    } catch(err) {
      console.log("HLTV.getMatchMapStats error")
      console.dir(err)
    }

    // Download demo archive
    try {
      do {
        // Snoozes function without pausing event loop
        // console.log("concurDL: %d", concurDL)
        await snooze(1000)
      }
      while (concurDL >= 2)

        downloadMatch(MatchArr[i], MatchMapStatsArr[i]).then(async fulfilled => {
          console.log(`Importing demos for Match: ${MatchArr[i].id}`)
          for (let d=0; d < fulfilled.demos.length; d++) {
            // Is the current MatchMapStats appropriate for the demo?
            var haveMapStats = fulfilled.demos[d].match(config[MatchMapStatsArr[i].map])
            console.log(haveMapStats)
            var importMatchMapStats
            if (haveMapStats) {
              importMatchMapStats = MatchMapStatsArr[i]
            } else { // If not, check orphans
              var matchingOrphans = orphanMapStats.filter(ms => {
                var sameMap = fulfilled.demos[d].match(config[ms.map])
                return ((ms.matchPageID == MatchArr[i].id) && sameMap)
              })
              if (matchingOrphans.length == 1) {
                importMatchMapStats = matchingOrphans[0]
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
              console.log(`Import for ${MatchArr[i].id}-${fulfilled.demos[d]} waiting. . . curImport = ${curImport}`)
            }
            while (curImport)

            curImport = MatchArr[i].id + "|" + fulfilled.demos[d]
            await importDemo(fulfilled.out_dir + fulfilled.demos[d], importMatchMapStats, MatchArr[i])
            curImport = ""
          }
          // After importing all demos for the match, remove the orphan(s) used.
          orphanMapStats = orphanMapStats.filter(ms => ms.matchPageID != MatchArr[i].id)
        })
        .catch(err => console.log(err))
    } catch (err) {
      console.log(err)
    }
  }
}

setTimeout(function() {
}, 5000)

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms))


async function downloadMatch(Match, MatchMapStats){
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
        orphanMapStats.push(MatchMapStats)
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
