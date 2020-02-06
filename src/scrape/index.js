const fs = require('fs')
var Promise = require('bluebird')
const apiConfig = require('hltv').default.config
const FetchStream = require('fetch').FetchStream
const MapDict = require('./maps.json')
const { extractArchive } = require('./utils.js')
const { importDemo, importMatch } = require('./import.js')
const { exec } = require('child_process')
var moment = require('moment')
const { getMatchesStats, getMatchMapStats, getMatch, snooze, asyncForEach, checkDbForMap, checkDbForMatch } = require('./utils.js')

require('console-stamp')(console, 'mmm/dd/yyyy | HH:MM.l')

var orphanMapStats = []
var problemImports = []
var concurDL = 0 // TODO(jcm): Change to array to track mms/match ids.
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
    } else if (dbHasMap.length === 1) {
      console.log(`${dbHasMap.length} entries already in Map table. skipping ${matchStats.id}|`)
      orphanMapStats.push({ json: null, MapStatsID: matchStats.id, matchPageID: null, map: matchStats.map, skip: true })
      mapsInDb = mapsInDb + 1
      return null // next forEach
    } else {
      console.log(`Inspect ${matchStats.id}|`)
      console.log(matchStats)
    }

    var matchMapStats = await getMatchMapStats(matchStats)
    var mapDate = moment(matchMapStats.date).format('YYYY-MM-DD h:mm ZZ')
    var match = await getMatch(matchStats, matchMapStats.matchPageID)

    var dbHasMatch = await checkDbForMatch(matchStats, match)
    // If missing the match from DB, import the match
    if ((dbHasMatch.length === 0)) {
      do {
        // Snoozes function without pausing event loop
        if (curImport !== '') {
          // console.log(`Match import (${matchStats.id}|${match.id} waiting. . . curImport = ${curImport}`)
        }
        await snooze(1000)
        // console.log(`Import for ${matchArr[i].id}-${matchContent.demos[d]} waiting. . . curImport = ${curImport}`)
      }
      while (curImport !== '')

      curImport = match.id
      var matchImported = await importMatch(match, matchStats)
      curImport = ''
    }

    if ((dbHasMatch.length === 1) || (!matchImported)) {
      orphanMapStats.push({ json: matchMapStats, MapStatsID: matchStats.id, matchPageID: matchMapStats.matchPageID, map: matchMapStats.map })
      console.log(`Match already in table, skipping. . . ${matchStats.id}|${match.id}`)
      return null
    }

    // Don't import the match if db already has it, or the import failed
    // (usually due to pkey_error race condition since mms_ids for the same
    // match are close.

    // Download demo archive
    // Import the match data into SQL database, in case something goes wrong with the download or the import.
    do {
      // Snoozes function without pausing event loop
      if (concurDL >= 2) {
        // console.log(`Demo download halted. (${matchStats.id}|${match.id}) ${concurDL} DL's already occurring.`)
      }
      await snooze(1000)
    }
    while (concurDL >= 2)
    concurDL += 1

    try {
      var matchContent = await downloadMatch(match, matchMapStats, matchStats.id, matchStats)
      var matchDate = moment(match.date).format('YYYY-MM-DD h:mm ZZ')
    } catch (err) {
      console.log(`Error downloading? ${matchStats.id}|${match.id}`)
      console.log(err)
      if (matchContent === undefined) {
        console.log()
        problemImports.push({ match: match, matchMapStats: matchStats })
        return null
      }
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
          var sameMap = Boolean(demo.match(MapDict[ms.map])[0])
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
          var missingMapStats = match.maps.filter(map =>
            (Boolean(demo.match(MapDict[map.name])[0]) && map.statsId !== undefined))
          if (missingMapStats.length === 1) {
            missingMapStats = missingMapStats[0]
            importMatchMapStatsID = missingMapStats.statsId

            importMatchMapStats = await getMatchMapStats(importMatchMapStatsID)
          } else {
            console.log(`Orphan fetching error. |${match.id}|${matchDate}`)
            console.log(match.maps)
            console.log(missingMapStats)
            console.log(demo)
            return null // Skip importing this demo
          }
          console.log(`Fetched matchMapStats. (No orphans found.) ${importMatchMapStatsID}|${match.id}|${matchDate}`)
        }
      }

      do {
        // Snoozes function without pausing event loop
        if (curImport !== '') {
          console.log(`Demos import (${importMatchMapStatsID}|${match.id} waiting. . . curImport = ${curImport}`)
        }
        await snooze(1000)
      }
      while (curImport !== '')

      curImport = importMatchMapStatsID + '|' + match.id
      var demoImportSuccess = await importDemo(matchContent.outDir + demo, importMatchMapStats, importMatchMapStatsID,
        match
      )
      curImport = ''
      numMapImports += demoImportSuccess
      if (demoImportSuccess === false) {
        // TODO(jcm): make table for this, chance to try out sequelize only row inserts?
        console.log(`Error importing demo ${importMatchMapStatsID}|${match.id}`)
        problemImports.push({ match: match, matchMapStats: importMatchMapStats })
      }
      console.log(`${numMapImports + mapsInDb}/${matchesStats.length} demos now in Maps table [${dateStr}].`)
      // Remove .dem file (it's sitting in the .rar archive anyway), can optionally kill
      exec(`rm ${matchContent.outDir + demo}`)
      // .then((success) => {
      // curImport = ''
      // numMapImports = numMapImports + success
      // console.log(`${numMapImports + mapsInDb}/${matchesStats.length} demos imported.`)
      // Remove .dem file (it's sitting in the .rar archive anyway), can optionally kill
      // exec(`rm ${matchContent.outDir + demo}`)
      // console.log(success)
      // })
      // .catch((err) => {
      // console.log(`Error importing demo ${importMatchMapStatsID}|${match.id}`)
      // console.log(err)
      // TODO(jcm): make table for this, chance to try out sequelize only row inserts?
      // problemImports.push({ match: match, matchMapStats: importMatchMapStats })
      // })
    })
    // Optionally remove .rar archive?
    // exec(`rm -rf ${matchContent.outDir + 'archive.rar'}`)
    console.log(`Done with |${match.id}`)
    return null
  }))
  console.log(`${numMapImports + mapsInDb}/${matchesStats.length} maps imported for ${dateStr}. (${mapsInDb} maps already in DB.)`)
  // return {
  // imports: numMapImports,
  // total: matchesStats.length,
  // mapsInDb: mapsInDb,
  // mapArr: mapArr
  // }
}

async function downloadMatch (match, matchMapStats, matchMapStatsID, matchStats) {
  // concurDL += 1
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
          console.log(`Extracted demos found. ${matchStats.id}|${match.id} `)
          concurDL -= 1
          resolve({
            outDir: outDir,
            demos: demos
          })
        } else {
          // console.log(`Re-extracting demos ${matchStats.id}|${match.id}`)
          demos = await extractArchive(outPath, outDir, match.id)
          concurDL -= 1
          resolve({
            outDir: outDir,
            demos: demos
          })
        }
      })
    } else { // If archive is not done downloading
      var out = fs.createWriteStream(outPath, { flags: 'w' }) // Overwrites incomplete archives
        .on('error', (e) => {
        })
        .on('ready', () => {
          console.log(`Starting download. . . ${matchMapStatsID}|${match.id} {${concurDL} DL's now}`)

          new FetchStream(demoLink)
            .pipe(out)
            .on('error', (err) => {
              concurDL -= 1
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
              fs.writeFile(outDir + 'dlDone.txt', `Downloaded ${moment().format('YYYY-MM-DD h:mm ZZ')}`, function (err) {
                if (err) throw err
                console.log(`Archive downloaded ${matchMapStatsID}|${match.id} {${concurDL} DL's now}`)
              })
              concurDL -= 1
              resolve({
                outDir: outDir,
                demos: demos || undefined
              })
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

// downloadDays('2019-10-31', '2019-11-21')
// downloadDays('2019-10-31', '2019-11-30')

try {
  downloadDays('2019-09-01', '2019-12-31')
} catch (err) {
  console.log(err)
}
