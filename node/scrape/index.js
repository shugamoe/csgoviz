const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
const fetch = require('node-fetch')
fetch.Promise = Promise
const config = require('./config.json')
const { extractArchive } = require('./utils.js')
const { RateLimiterMemory, RateLimiterQueue } = require('rate-limiter-flexible');

const http = require('https')


const queryRLM = new RateLimiterMemory({
    points: 1,
    duration: 3,
});
const queryLimiter = new RateLimiterQueue(queryRLM, {
    maxQueueSize: 2,
})

function sleep(seconds) {
  var startTime = new Date().getTime();
  while (new Date().getTime() < startTime + (1000 * seconds));
}

var concurDL = 0
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

  for (let i = 0; i < MatchesStats.length; i++){
    try {
      const remainingTokens = await queryLimiter.removeTokens(1)
      var MatchMapStats = await HLTV.getMatchMapStats({
        id: MatchesStats[i].id
      })
    } catch (err) {
      console.log("HLTV.getMatchMapStats error")
      console.dir(err)
    }

    try {
      const remainingTokens = await queryLimiter.removeTokens(1)
      var Match = await HLTV.getMatch({
        id: MatchMapStats.matchPageID
      })
    } catch(err) {
      console.log("HLTV.getMatchMapStats error")
      console.dir(err)
    }

    // Download demo archive
    try {
      do {
        // Snoozes function without pausing event loop
        console.log("concurDL: %d", concurDL)
        await snooze(1000)
      }
      while (concurDL >= 2)
      downloadMatch(Match, MatchMapStats)
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
      .on('error', (err) => {
        console.log("%d already downloaded or downloading (%s), skipping. . .", Match.id, out_path)
        concurDL -= 1
        resolve()
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
              await extractArchive(out_path, out_dir)
            } catch(err) {
              console.dir(err)
            }
            resolve()
          })
      })
  })
}

var test_getMatchesStats = JSON.parse(fs.readFileSync('./test_getMatchesStats.txt', 'utf8'))
var test_getMatchMapStats = JSON.parse(fs.readFileSync('./test_getMatchMapStats.txt', 'utf8'))
var test_getMatch = JSON.parse(fs.readFileSync('./test_getMatch.txt'))
// downloadMatch(test_getMatch, test_getMatchMapStats)
downloadDay('2019-11-02')
