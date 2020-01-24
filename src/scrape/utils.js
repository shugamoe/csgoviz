const { unrar, list } = require('unrar-promise')
const { HLTV } = require('hltv')
const matchType = require('hltv').MatchType
var Promise = require('bluebird')
const moment = require('moment')
var Models = require('./models.js')
var db = require('./db.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible')

const queryRLM = new RateLimiterMemory({
  points: 1,
  duration: 3 // query limit 1 per 3 seconds (robots.txt has 1 sec)
})
const queryLimiter = new RateLimiterQueue(queryRLM, {
  maxQueueSize: 1000
})

setTimeout(function () {
}, 5000)

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
    numRetries = 6
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
    if (numRetries === 0){
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

async function getMatchMapStats(matchStats, numRetries){
  if (numRetries === undefined) {
    numRetries = 6
  }
  var mapDate
  if (typeof(matchStats) == 'number') {
    matchStats = {
      id: matchStats,
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
    if (numRetries === 0){
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

async function getMatch(matchStats, matchId, numRetries){
  if (numRetries === undefined) {
    numRetries = 6
  }

  try {
    await queryLimiter.removeTokens(1)
    var match = await HLTV.getMatch({
      id: matchId
    })
  } catch (err) {
    var mapDate = moment(matchStats.date).format('YYYY-MM-DD h:mm:ss ZZ')
    console.log(err)
    if (numRetries === 0){
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

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

async function checkDbForMap(matchStats){
  return new Promise(async (resolve, reject) => {
    try {
      var dbHasMap = await Models.Map.findAll({
              attributes: ['mms_id'],
        where: { mms_id: matchStats.id }
      })
    } catch(err) {
      console.log(`Map table check error. ${matchStats.id}`)
    }
    resolve(dbHasMap)
  })
}

async function checkDbForMatch(matchStats, match){
  return new Promise(async (resolve, reject) => {
    try {
      var dbHasMatch = await Models.Match.findAll({
              attributes: ['match_id'],
        where: { match_id: match.id }
      })
    } catch(err) {
      console.log(`Match table check error. ${matchStats.id}|${match.id}`)
    }
    resolve(dbHasMatch)
  })
}

async function clearMatches(){
  // Super paranoid about await and promises here, might be overkill but I've
  // seen evidence of async functions or promise returning functions called
  // with await NOT waiting. . .
  // Catching flak for promise executor functions should not be async, but it works.
  // TODO(jcm): investigate later ^
  db.sync()
  return new Promise((resolve, reject) => {
    try {
      var rowsDeleted = Models.Match.destroy({ where: {}, truncate: true })
    } catch (err) {
      console.log('Error deleting matches table')
    }
    resolve(rowsDeleted)
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
}
// extractArchive(tarchPath)
