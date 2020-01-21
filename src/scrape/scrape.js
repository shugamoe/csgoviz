const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
const MapDict = require('./maps.json')
const { extractArchive } = require('./utils.js')
const { importDemo, importMatch } = require('./import.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible')
var Models = require('./models.js')
const { exec } = require('child_process')
var moment = require('moment')
require('console-stamp')(console, 'mmm/dd/yyyy | HH:MM:ss.l')
// var pg = require('pg')
// var config = require('../import/config.json')
// var client = new pg.Client(config.connectionString)

const queryRLM = new RateLimiterMemory({
  points: 1,
  duration: 3
})
const queryLimiter = new RateLimiterQueue(queryRLM, {
  maxQueueSize: 1000
})

var orphanMapStats = []
var problemImports = []
var concurDL = 0
var curImport = ''
async function downloadDay (dateStr) {
  try {
    await queryLimiter.removeTokens(1)
    var matchesStats = await HLTV.getMatchesStats({
      startDate: dateStr,
      // TODO(jcm): Test longer time-periods and/or confirm how gigobyte/hltv
      // handles many matchesStat results (rate-limit concern)
      endDate: dateStr,
      matchType: matchType.LAN
    })
    console.log(`Starting ${dateStr}`)
  } catch (err) {
    console.log(`${dateStr} HLTV.getMatchesStats error`)
    console.dir(err)
  }

  console.log(`${matchesStats.length} results for ${dateStr}`)

  // Do not download/import new maps if mms_id in Map table (save HLTV load by
  // catching before further HLTV requests are made)
  matchesStats.forEach(async (matchStats, msi, arr) => {
    var dbHasMap = await Models.Map.findAll({
      attributes: ['mms_id'],
      where: { mms_id: matchStats.id }
    })
    if (dbHasMap.length === 0) { // If we don't have that mms_id in the Maps
      // console.log(`mms_id:${matchStats.id} not in Map table`)
      // TODO(jcm): maybe gather up some of these commented out comments to be a debug=true print only?
    } else {
      console.log(`${matchStats.id}| has ${dbHasMap.length} entries in Maps already, skipping. . . `)
      // Indicate that we don't want to re-import this, might be necessary
      // if 1 or more maps from match is in DB, but another is missing,
      // don't need to re-import something we already have TODO(jcm):
      // perhaps compare hashes of the file?
      orphanMapStats.push({ json: null, MapStatsID: matchStats.id, matchPageID: null, map: matchStats.map, skip: true})
      return null
    }
    // Cap at 16 per day for now
    // if (msi >= 16) {
      // return null
    // }
    try {
      await queryLimiter.removeTokens(1)
      var matchMapStats = await HLTV.getMatchMapStats({
        id: matchStats.id
      })
      var mapDate = new Date(matchMapStats.date).toUTCString()
    } catch (err) {
      console.log(`${matchStats.id}|${matchMapStats.matchPageID}|${mapDate} HLTV.getMatchMapStats error`)
      console.dir(err)
    }

    try {
      await queryLimiter.removeTokens(1)
      var match = await HLTV.getMatch({
        id: matchMapStats.matchPageID
      })
    } catch (err) {
      console.log(`${matchStats.id}|${matchMapStats.matchPageID}|${mapDate} HLTV.getMatch error`)
      console.dir(err)
    }

    // Download demo archive
    // Import the match data into SQL database, in case something goes wrong with the download or the import.
    do {
      // Snoozes function without pausing event loop
      await snooze(1000)
    }
    while (curImport)

    var dbHasMatch = await Models.Match.findAll({
      attributes: ['match_id'],
      where: { match_id: match.id }
    })
    if (dbHasMatch.length === 0) { // If we don't have that match_id in the Match table
      curImport = match.id
      await importMatch(match).then(curImport = '')
    } else {
      orphanMapStats.push({ json: matchMapStats, MapStatsID: matchStats.id, matchPageID: matchMapStats.matchPageID, map: matchMapStats.map})
      // With this only one map(Match)Stats id (mms_id) will trigger an attempt to download the demos
      // console.log(`${matchStats.id}|${match.id} sent to orphanMapStats. Already in Match table, skipping download. . .`)
      return null
    }

    do {
      // Snoozes function without pausing event loop
      // console.log("concurDL: %d", concurDL)
      await snooze(1000)
    }
    while (concurDL >= 2)

    downloadMatch(match, matchMapStats, matchStats.id, matchStats).then(async fulfilled => {
      fulfilled.demos.forEach(async (demo) => {
        // Is the current matchMapStats appropriate for the demo?
        var haveMapStats = demo.match(MapDict[matchMapStats.map])
        var importMatchMapStats
        var importMatchMapStatsID
        if (haveMapStats) {
          importMatchMapStats = matchMapStats
          importMatchMapStatsID = matchStats.id
        } else { // If not, check orphans
          var matchingOrphans = orphanMapStats.filter(ms => {
            var mmsIdInMatch = match.maps.filter(map => map.statsId === ms.MapStatsID).length === 1
            var sameMatch = ms.matchPageID === match.id
            var sameMap = demo.match(MapDict[ms.map])
            return (sameMap && (mmsIdInMatch || sameMatch))
          })
          if (matchingOrphans.length >= 1) {
            if (matchingOrphans[0].skip === true) { // only happens if mms_id already in Map table
              console.log(`Orphan for ${matchStats.id}|${match.id} found, but mms_id is already in Map table, skipping import. . .`)
              return null // Skip importing this demo
            }
            console.log(`${matchStats.id}|${match.id} ${matchingOrphans.length} Orphan(s) found`)
            importMatchMapStats = matchingOrphans[0].json
            importMatchMapStatsID = matchingOrphans[0].MapStatsID

            // Clear mms_id from Orphans
            orphanMapStats = orphanMapStats.filter(ms => ms.MapStatsID !== importMatchMapStatsID)
          } else {
            // Go download the matchMapStats real quick (could be possible
            // if games in match were played before/after midnight
            // console.log(demo)
            // console.log(orphanMapStats)
            var missingMapStats = match.maps.filter(map => demo.match(MapDict[map.name]))
            if (missingMapStats.length === 1) {
              missingMapStats = missingMapStats[0]
              await queryLimiter.removeTokens(1)
              importMatchMapStats = await HLTV.getMatchMapStats({
                id: missingMapStats.statsId
              })
              importMatchMapStatsID = missingMapStats.statsId
            }
            var matchDate = moment(match.date).format('YYYY-MM-DD')
            console.log(`${importMatchMapStatsID}|${match.id}|${matchDate} Fetched matchMapStats. (No orphans found.)`)
          }
        }

        do {
          // Snoozes function without pausing event loop
          await snooze(1000)
          // console.log(`Import for ${matchArr[i].id}-${fulfilled.demos[d]} waiting. . . curImport = ${curImport}`)
        }
        while (curImport)

        curImport = importMatchMapStatsID + '|' + match.id
        await importDemo(fulfilled.outDir + demo, importMatchMapStats, importMatchMapStatsID, match).then(() => {
          curImport = ''
        }).catch((err) => {
          console.dir(`${importMatchMapStatsID}|${match.id} Error importing demo`)
          console.log(err)
          problemImports.push({ match: match, matchMapStats: importMatchMapStats })
        })
      })
    })
      .catch((err) => {
        console.log(`${matchStats.id}|${matchMapStats.matchPageID}|${mapDate}`)
        console.log(err)
      })
  })
}

setTimeout(function () {
}, 5000)

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms))

async function downloadMatch (match, matchMapStats, matchMapStatsID, matchStats) {
  var demoLink = match.demos.filter(demo => demo.name === 'GOTV Demo')[0].link
  demoLink = apiConfig.hltvUrl + demoLink

  var outDir = '/home/jcm/matches/' + match.id + '/'
  var outPath = outDir + 'archive.rar'

  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true })
    // console.log("%d folder created.", match.id)
  }

  return new Promise((resolve, reject) => {
    concurDL += 1
    var out = fs.createWriteStream(outPath, { flags: 'wx' })
      .on('error', (e) => {
        // TODO(jcm): Can check download status (or file stream activity?) of
        // .rar archives and .dem files to not have to re-download (HLTV load)
        // or re-extract (user performance). Rejecting for now
        concurDL -= 1
        console.log(`${matchStats.id}|${match.id} already downloaded or downloading (${outPath}), skipping. . .`)
        reject(e)
      })
      .on('ready', () => {
        console.log(`${matchMapStatsID}|${match.id} starting download. . .`)

        new FetchStream(demoLink)
          .pipe(out)
          .on('error', err => console.log(err, null)) // Could get 503 (others too possib.) log those for checking later?
          .on('finish', async () => {
            console.log(`${matchMapStatsID}|${match.id} archive downloaded`)
            concurDL -= 1
            try {
              var demos = await extractArchive(outPath, outDir, match.id)
            } catch (err) {
              console.dir(err)
            }
            resolve({
              outDir: outDir,
              demos: demos || undefined
            }
            )
          })
      })
  })
}

// Rudimentary function to download a lot of days
async function downloadDays (startDateStr, endDateStr) {
  var startDate = moment(startDateStr)
  var endDate = moment(endDateStr)
  var dlDate = startDate
  while (dlDate.toString() !== endDate.toString()) {
    await downloadDay(dlDate.format('YYYY-MM-DD'))
    dlDate.add(1, 'd')
  }
  var thing = await downloadDay(endDate.format('YYYY-MM-DD'))
  console.log(`Downloaded from ${startDateStr} to ${endDateStr}`)
}

// var test_getMatchesStats = JSON.parse(fs.readFileSync('./test_getMatchesStats.txt', 'utf8'))
// var test_getMatchMapStats = JSON.parse(fs.readFileSync('./test_getMatchMapStats.txt', 'utf8'))
// var test_getMatch = JSON.parse(fs.readFileSync('./test_getMatch.txt'))
// downloadMatch(test_getMatch, test_getMatchMapStats)
// downloadDay('2019-11-02').then(() => {
// exec('rm -rf ~/matches/*.dem')
// exec('rm -rf ~/matches/*.rar')
// })
//

downloadDays('2019-10-31', '2019-11-07')
