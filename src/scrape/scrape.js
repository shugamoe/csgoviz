const fs = require('fs')
var Promise = require('bluebird')
const apiConfig = require('hltv').default.config
const FetchStream = require('fetch').FetchStream
const MapDict = require('./maps.json')
const { extractArchive } = require('./utils.js')
const { importDemo, importMatch } = require('./import.js')
const { exec } = require('child_process')
var moment = require('moment')
const { getMatchesStats, getMatchMapStats, getMatch, snooze, asyncForEach, checkDbForMap, checkDbForMatch} = require('./utils.js')

require('console-stamp')(console, 'mmm/dd/yyyy | HH:MM:ss.l')

var orphanMapStats = []
var problemImports = []
var concurDL = 0
var curImport = ''

async function downloadDay (dateStr) {
  // TODO(jcm): Have restart functionality in case the catch blocks are
  // encountered (happens after a while? Use basic recursive fn or something)
  var matchesStats = await getMatchesStats(dateStr, dateStr)
  console.log(`${matchesStats.length} results for ${dateStr}`)
  var numMapImports = 0
  var mapsInDb = 0

  // Do not download/import new maps if mms_id in Map table (save HLTV load by
  // catching before further HLTV requests are made)
  // await matchesStats.forEach(async (matchStats, msi, arr) => {
  // await asyncForEach(matchesStats, async (matchStats, msi, arr) => {
  await Promise.all(matchesStats.map(async (matchStats, msi, arr) => {
    var dbHasMap = await checkDbForMap(matchStats)
    if (dbHasMap.length === 0) { // If we don't have that mms_id in the Maps
      console.log(`Not in Map table. ${matchStats.id}`)
      // TODO(jcm): maybe gather up some of these commented out comments to be a debug=true print only?
    } else if (dbHasMap.length === 1){
      console.log(`${dbHasMap.length} entries already in Map table. skipping ${matchStats.id}|`)
      orphanMapStats.push({ json: null, MapStatsID: matchStats.id, matchPageID: null, map: matchStats.map, skip: true })
      mapsInDb = mapsInDb + 1
      return null // next forEach
    } else {
      console.log(`Inspect ${matchStats.id}|`)
      console.log(matchStats)
    }

    var matchMapStats = await getMatchMapStats(matchStats)
    var mapDate = moment(matchMapStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
    var match = await getMatch(matchStats, matchMapStats.matchPageID)


    // Download demo archive
    // Import the match data into SQL database, in case something goes wrong with the download or the import.

    var dbHasMatch = await checkDbForMatch(matchStats, match)
    if (matchStats.id === 94246) {
      console.log(`Problem mms_id: 94246 ${dbHasMap}`)
    }
    if (dbHasMatch.length === 0) { // If we don't have that match_id in the Match table
      do {
        // Snoozes function without pausing event loop
        await snooze(1000)
        // console.log(`Import for ${matchArr[i].id}-${matchContent.demos[d]} waiting. . . curImport = ${curImport}`)
      }
      while (curImport)

      curImport = match.id
      await importMatch(match).then(curImport = '')
    } else {
      // if ((matchStats.id === 94246)){
        // console.log(dbHasMap)
        // console.log(`1 match, one map ${matchStats.id}|${match.id}`)
      // }
      orphanMapStats.push({ json: matchMapStats, MapStatsID: matchStats.id, matchPageID: matchMapStats.matchPageID, map: matchMapStats.map })
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

    try {
      var matchContent = await downloadMatch(match, matchMapStats, matchStats.id, matchStats)
      var matchDate = moment(match.date).format('YYYY-MM-DD h:mm:ss ZZ')
    } catch (err) {
      console.log(`Error downloading? ${matchStats.id}|${match.id}`)
      console.log(err)
    }
    // matchContent.demos.forEach(async (demo) => {
    await asyncForEach(matchContent.demos, async (demo) => {
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
          importMatchMapStats = matchingOrphans[0].json
          importMatchMapStatsID = matchingOrphans[0].MapStatsID
          console.log(`${matchingOrphans.length} Orphan(s) found for ${importMatchMapStatsID}|${match.id}`)

          // Clear entry from Orphans
          orphanMapStats = orphanMapStats.filter(ms => ms.MapStatsID !== importMatchMapStatsID)
        } else {
          // Go download the matchMapStats real quick (could be possible
          // if games in match were played before/after midnight
          // console.log(demo)
          // console.log(orphanMapStats)
          var missingMapStats = match.maps.filter(map => demo.match(MapDict[map.name]))
          if (missingMapStats.length === 1) {
            missingMapStats = missingMapStats[0]
            importMatchMapStatsID = missingMapStats.statsId

            importMatchMapStats = await getMatchMapStats(matchStats)
          } else {
            console.log(`Orphan fetching error. |${match.id}|${matchDate}`)
            console.log(match.maps)
            console.log(demo)
          }
          console.log(`Fetched matchMapStats. (No orphans found.) ${importMatchMapStatsID}|${match.id}|${matchDate}`)
        }
      }

      do {
        // Snoozes function without pausing event loop
        await snooze(1000)
        // console.log(`Import for ${matchArr[i].id}-${matchContent.demos[d]} waiting. . . curImport = ${curImport}`)
      }
      while (curImport)

      curImport = importMatchMapStatsID + '|' + match.id
      await importDemo(matchContent.outDir + demo, importMatchMapStats, importMatchMapStatsID,
        match
      )
        .then(() => {
          curImport = ''
          numMapImports = numMapImports + 1
        })
        .catch((err) => {
          console.log(`Error importing demo ${importMatchMapStatsID}|${match.id}`)
          console.log(err)
          // TODO(jcm): make table for this, chance to try out sequelize only row inserts?
          problemImports.push({ match: match, matchMapStats: importMatchMapStats })
        })
      // Remove .dem file (it's sitting in the .rar archive anyway), can optionally kill
      exec(`rm ${matchContent.outDir + demo}`)
    })
      // Optionally remove .rar archive?
      // exec(`rm -rf ${matchContent.outDir + 'archive.rar'}`)
      .catch((err) => {
        console.log(`Error in downloadMatch. ${matchStats.id}|${matchMapStats.matchPageID}|${mapDate}`)
        console.log(err)
      })
    return null
  }))
    .then(res => {
      console.log(`${numMapImports + mapsInDb}/${matchesStats.length} maps imported for ${dateStr}. (${mapsInDb} maps already in DB.)`)
      console.log(res)
      return new Promise((resolve, reject) => {
        return {
          imports: numMapImports,
          total: matchesStats.length,
          mapsInDb: mapsInDb,
        }
      })
    })
  // return {
    // imports: numMapImports,
    // total: matchesStats.length,
    // mapsInDb: mapsInDb,
    // mapArr: mapArr
  // }
}

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
    if (fs.existsSync(outDir + 'dlDone.txt')) { // If the archive is already downloaded.
      console.log(`A previous download is complete. Attempting to locate extracted demos. ${matchStats.id}|${match.id}`)
      fs.readdir(outDir, async (err, files) => {
        if (err) {
          console.log(`${matchStats.id}|${match.id} Error reading directory ${outDir}`)
          console.log(err)
        }

        var demos = files.filter(f => f.substr(f.length - 3) === 'dem')
        if (demos.length > 0) {
          console.log(`${matchStats.id}|${match.id} Extracted demos found.`)
          resolve({
            outDir: outDir,
            demos: demos
          })
        } else {
          console.log(`Re-extracting demos ${matchStats.id}|${match.id}`)
          demos = await extractArchive(outPath, outDir, match.id)
          resolve({
            outDir: outDir,
            demos: demos
          })
        }
      })
    } else { // If archive is not done downloading
      concurDL += 1
      var out = fs.createWriteStream(outPath, { flags: 'w' }) // Overwrites incomplete archives
        .on('error', (e) => {
        })
        .on('ready', () => {
          console.log(`Starting download. . . ${matchMapStatsID}|${match.id}`)

          new FetchStream(demoLink)
            .pipe(out)
            .on('error', (err) => {
              console.log(`File download error. Removing incomplete archive. ${matchMapStatsID}|${match.id}`)
              console.log(err)
              fs.unlink(outPath, (err) => {
                console.log(`Error deleting incomplete archive download. ${matchMapStatsID}|${match.id}`)
                console.log(err)
              })
            }) // Could get 503 (others too possib.) log those for checking later?
            .on('finish', async () => {
              var demos = await extractArchive(outPath, outDir, match.id)
              // Make quick flag file to show that it's complete
              fs.writeFile(outDir + 'dlDone.txt', `Downloaded ${moment().format('YYYY-MM-DD h:mm:ss ZZ')}`, function (err) {
                if (err) throw err
                console.log(`Archive downloaded ${matchMapStatsID}|${match.id}`)
              })
              concurDL -= 1
              return {
                outDir: outDir,
                demos: demos || undefined
              }
            })
        })
    }
  })
}

// Rudimentary function to download a lot of days
async function downloadDays (startDateStr, endDateStr) {
  var startDate = moment(startDateStr)
  var endDate = moment(endDateStr)
  var deltaDays = moment.duration(endDate.diff(startDate)).days()
  var addDays = Array.from(Array(deltaDays + 1).keys()) // so we can use forEach

  // addDays.forEach(async (days) => {
  await asyncForEach(addDays, async (days) => {
    var dlDate = moment(startDateStr).add(days, 'd').format('YYYY-MM-DD')
    await downloadDay(dlDate)
  })
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

// downloadDays('2019-10-31', '2019-11-21')
downloadDays('2019-10-31', '2019-10-31')
