const homeDir = require('os').homedir()
const fs = require('fs')
const { exec } = require('child_process')
const { unrar, list } = require('unrar-promise')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const FetchStream = require('fetch').FetchStream
const matchType = require('hltv').MatchType
var Promise = require('bluebird')
const moment = require('moment')
var Models = require('./models.js')
var db = require('./db.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible')
const { Op } = require('sequelize')
const MapDict = require('./maps.json')
const { importDemo } = require('./import.js')
const defaultRetries = 2

const queryRLM = new RateLimiterMemory({
  points: 1,
  duration: 3 // query limit 1 per 3 seconds (robots.txt has 1 sec)
})
const queryLimiter = new RateLimiterQueue(queryRLM, {
  maxQueueSize: 1000
})

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms))

async function extractArchive (archPath, targetDir, matchID) {
  console.log(`Extracting . . . |${matchID}`)
  try {
    await unrar(archPath, targetDir, { overwrite: true })
    return list(archPath)
  } catch (err) {
    console.log(`Extraction err |${matchID}`)
    return err
  }
}

async function getMatchesStats (startDate, endDate, numRetries) {
  if (numRetries === undefined) {
    numRetries = defaultRetries
  }

  try {
    await queryLimiter.removeTokens(1)
    var matchesStats = await HLTV.getMatchesStats({
      startDate: startDate,
      endDate: endDate,
      matchType: matchType.LAN
    })
    console.log(`Starting ${startDate} to ${endDate}`)
  } catch (err) {
    console.log(err)
    if (numRetries === 0) {
      console.log(`HLTV.getMatchesStats error (no more retries). ${startDate}-${endDate}`)
      return null
    } else {
      console.log(`HLTV.getMatchesStats (${numRetries} more retries). ${startDate}${endDate}`)
      snooze(1200000) // 20 minutes
      return getMatchesStats(startDate, endDate, numRetries - 1)
    }
  }
  return matchesStats
}

async function getMatchMapStats (matchStats, numRetries) {
  if (numRetries === undefined) {
    numRetries = defaultRetries
  }
  var mapDate
  if (typeof (matchStats) === 'number') {
    matchStats = {
      id: matchStats
    }
    mapDate = '[Date N/A]'
  } else {
    mapDate = moment(matchStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
  }

  try {
    await queryLimiter.removeTokens(1)
    var matchMapStats = await HLTV.getMatchMapStats({
      id: matchStats.id
    })
  } catch (err) {
    console.log(err)
    if (numRetries === 0) {
      console.log(`HLTV.getMatchMapStats error. (no more retries) ${matchStats.id}||${mapDate}`)
      return null
    } else {
      console.log(`HLTV.getMatchMapStats error. (${numRetries} more retries) ${matchStats.id}||${mapDate}`)
      snooze(1200000) // 20 minutes
      return getMatchMapStats(matchStats, numRetries - 1)
    }
  }
  return matchMapStats
}

async function getMatch (matchStats, matchId, numRetries) {
  var mapDate
  if (typeof (matchStats) === 'number') {
    matchStats = {
      id: matchStats
    }
    mapDate = '[Date N/A]'
  } else {
    mapDate = moment(matchStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
  }
  if (numRetries === undefined) {
    numRetries = defaultRetries
  }

  try {
    await queryLimiter.removeTokens(1)
    var match = await HLTV.getMatch({
      id: matchId
    })
  } catch (err) {
    var mapDate = moment(matchStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
    console.log(err)
    if (numRetries === 0) {
      console.log(`HLTV.getMatch error (no more retries) ${matchStats.id}|${matchId}|${mapDate}`)
      return null
    } else {
      console.log(`HLTV.getMatch error (${numRetries} more retries) ${matchStats.id}|${matchId}|${mapDate}`)
      snooze(1200000) // 20 minutes
      return getMatch(matchStats, matchId, numRetries - 1)
    }
  }
  return match
}

async function asyncForEach (array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array)
  }
}

async function checkDbForMap (matchStats) {
  var mapDate
  if (typeof (matchStats) === 'number') {
    matchStats = {
      id: matchStats
    }
    mapDate = '[Date N/A]'
  } else {
    mapDate = moment(matchStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
  }

  return new Promise(async (resolve, reject) => {
    try {
      var dbHasMap = await Models.Map.findAll({
        attributes: ['mms_id'],
        where: { mms_id: matchStats.id }
      })
    } catch (err) {
      console.log(`Map table check error. ${matchStats.id}`)
    }
    resolve(dbHasMap)
  })
}

async function checkDbForMatch (matchStats, match) {
  return new Promise(async (resolve, reject) => {
    try {
      var dbHasMatch = await Models.Match.findAll({
        attributes: ['match_id'],
        where: { match_id: match.id }
      })
    } catch (err) {
      console.log(`Match table check error. ${matchStats.id}|${match.id}`)
    }
    resolve(dbHasMatch)
  })
}

async function clearMatches () {
  // Super paranoid about await and promises here, might be overkill but I've
  // seen evidence of async functions or promise returning functions called
  // with await NOT waiting. . .
  // Catching flak for promise executor functions should not be async, but it works.
  // TODO(jcm): investigate later ^
  return db.sync().then(() => {
    return new Promise(async (resolve, reject) => {
      try {
        var rowsDeleted = await Models.Match.destroy(
          {
            where:
            {
              maps_played:
              { [Op.gt]: 1 }
            },
            truncate: true
          })
        console.log('Matches table cleared.')
      } catch (err) {
        console.log('Error deleting matches table')
        console.log(err)
      }
      resolve(rowsDeleted)
    })
  })
}

async function auditDB (options) {
  var concurDL = 0
  // var curImport = ''
  var curImport = 0
  var totalMissingMaps = 0
  var auditMapImports = 0

  if (options === undefined) {
    options = {
      maxImports: 1,
      maxDLs: 2
    }
  }
  // Find matches that are missing maps. matches.maps_played is the number of
  // maps we expect with a given match_id
  const [results, _] = await db.query(`
    SELECT matches.match_id, matches.data, matches.maps_played - COALESCE(T1.maps_in_db, 0) AS maps_missing
    FROM matches 
      LEFT OUTER JOIN (SELECT match_id, COUNT(*) AS maps_in_db 
                       FROM maps 
                       GROUP BY match_id) AS T1
        ON T1.match_id = matches.match_id
    WHERE matches.maps_played > T1.maps_in_db
    OR matches.maps_played - T1.maps_in_db IS NULL
    ORDER BY matches.date DESC
    `)

  await Promise.all(results.map(async (res) => {
    totalMissingMaps += parseInt(res.maps_missing)
  }))
  console.log(`${results.length} matches with ${totalMissingMaps} missing maps.`)
  // asyncForEach(results, async (res) => {
  await Promise.all(results.map(async (res, index) => {
    if (index > 15) {
      return 'skipped'
    }

    do {
      // Snoozes function without pausing event loop
      if (concurDL >= options.maxDLs) {
        // console.log(`Demo download halted. (${matchStats.id}|${match.id}) ${concurDL} DL's already occurring.`)
      }
      await snooze(1000)
    }
    while (concurDL >= options.maxDLs)
    concurDL += 1

    try {
      var matchContent = await downloadMatch(res.data, 'auditDB')
      var matchDate = moment(res.data.date).format('YYYY-MM-DD h:mm ZZ')
    } catch (err) {
      console.log(`Error downloading matches? |${res.data.id}`)
      console.log(err)
      if (matchContent === undefined) {
        // problemImports.push({ match: match, matchMapStats: matchStats })
        return null
      }
    } finally {
      concurDL -= 1
    }

    await asyncForEach(matchContent.demos, async (demo) => {
      var importMatchMapStats
      var importMatchMapStatsID
      var missingMapStats = res.data.maps.filter(map =>
        (Boolean(demo.match(MapDict[map.name])) && map.statsId !== undefined))

      if (missingMapStats.length === 1) {
        missingMapStats = missingMapStats[0]
        importMatchMapStatsID = missingMapStats.statsId
        var mapsInDb = await checkDbForMap(importMatchMapStatsID)
        if (mapsInDb.length === 1) {
          return // Already imported this map, ignore it
        } else {
          importMatchMapStats = await getMatchMapStats(importMatchMapStatsID)
        }
      } else {
        console.log(`Orphan fetching error. |${res.data.id}|${matchDate}`)
        console.log(res.data.maps)
        console.log(missingMapStats)
        console.log(demo)
        return null // Skip importing this demo
      }

      do {
        // Snoozes function without pausing event loop
        // if (curImport !== '') {
        if (curImport >= options.maxImports) {
          // console.log(`Demos import (${importMatchMapStatsID}|${match.id} waiting. . . curImport = ${curImport}`)
        }
        await snooze(1000)
      }
      // while (curImport !== '')
      while (curImport >= options.maxImports)

      // curImport = importMatchMapStatsID + '|' + res.data.id
      curImport += 1
      var demoImportSuccess = await importDemo(matchContent.outDir + demo, importMatchMapStats, importMatchMapStatsID, res.data)
      // curImport = ''
      curImport -= 1
      auditMapImports += demoImportSuccess
      if (demoImportSuccess === false) {
        // TODO(jcm): make table for this, chance to try out sequelize only row inserts?
        console.log(`Error importing demo ${importMatchMapStatsID}|${res.data.id}`)
        // problemImports.push({ match: res.data, matchMapStats: importMatchMapStats })
      }
      console.log(`${auditMapImports}/${totalMissingMaps} missing demos now in Maps table.`)
      // Remove .dem file (it's sitting in the .rar archive anyway), can optionally kill
      exec(`rm ${matchContent.outDir + demo}`)
    })
    // Is the current matchMapStats appropriate for the demo?
  }))
}

async function downloadMatch (match, matchMapStatsID, concurDL) {
  var demoLink = match.demos.filter(demo => demo.name === 'GOTV Demo')[0].link
  demoLink = apiConfig.hltvUrl + demoLink

  var outDir = homeDir + '/matches/' + match.id + '/'
  var outPath = outDir + 'archive.rar'

  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true })
    // console.log("%d folder created.", match.id)
  }

  return new Promise((resolve, reject) => {
    if (fs.existsSync(outDir + 'dlDone.txt')) { // If the archive is already downloaded.
      console.log(`Download is complete. Locating extracted demos. ${matchMapStatsID}|${match.id}`)
      fs.readdir(outDir, async (err, files) => {
        if (err) {
          console.log(`${matchMapStatsID}|${match.id} Error reading directory ${outDir}`)
          console.log(err)
        }

        var demos = files.filter(f => f.substr(f.length - 3) === 'dem')
        if (demos.length > 0) {
          console.log(`Extracted demos found. ${matchMapStatsID}|${match.id} `)
          resolve({
            outDir: outDir,
            demos: demos
          })
        } else {
          // console.log(`Re-extracting demos ${matchMapStatsID}|${match.id}`)
          demos = await extractArchive(outPath, outDir, match.id)
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
              // Make quick flag file to show that it's complete
              fs.writeFile(outDir + 'dlDone.txt', `Downloaded ${moment().format('YYYY-MM-DD h:mm ZZ')}`, function (err) {
                if (err) throw err
                console.log(`Archive downloaded ${matchMapStatsID}|${match.id}`)
              })
              var demos = await extractArchive(outPath, outDir, match.id)
              resolve({
                outDir: outDir,
                demos: demos || undefined
              })
            })
        })
    }
  })
}

module.exports = {
  extractArchive,
  getMatchesStats,
  getMatchMapStats,
  getMatch,
  queryLimiter,
  queryRLM,
  asyncForEach,
  snooze,
  checkDbForMap,
  checkDbForMatch,
  clearMatches,
  downloadMatch,
  auditDB
}
