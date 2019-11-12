const process = require('process')
const fs = require('fs')
var Promise = require('bluebird')
const { HLTV } = require('hltv')
const apiConfig = require('hltv').default.config
const matchType = require('hltv').MatchType
const FetchStream = require('fetch').FetchStream
const config = require('./config.json')
const { extractArchive } = require('./utils.js')

var test_getMatchesStats = JSON.parse(fs.readFileSync('./test_getMatchesStats.txt', 'utf8'))
var test_getMatchMapStats = JSON.parse(fs.readFileSync('./test_getMatchMapStats.txt', 'utf8'))
var test_getMatch = JSON.parse(fs.readFileSync('./test_getMatch.txt'))

function downloadMatch(Match, MatchMapStats){
  var demo_link = Match.demos.filter(demo => demo.name === 'GOTV Demo')[0].link
  demo_link = apiConfig.hltvUrl + demo_link

  var out_dir = '/home/jcm/matches/' + Match.id + '/'
  var out_path = out_dir + 'archive.rar'
  if (!fs.existsSync(out_dir)){
    fs.mkdirSync(out_dir, {recursive: true})
    console.log("%d now has a folder.", Match.id)
  }
  var out = fs.createWriteStream(out_path, {flags: 'wx'})
    .on('error', (err) => {
      console.dir(err)
      console.log("%d already downloaded (%s), skipping. . .", Match.id, out_path)
      extractArchive(out_path, out_dir)
    })
    .on('ready', () => {
      console.log("%d starting download. . .", Match.id) 
      new FetchStream(demo_link)
        .pipe(out)
        .on('error', err => console.log(err)) // Could get 503 (others too possib.) log those for checking later?
        .on('finish', () => {
          console.log("%d archive downloaded", Match.id)
        })
    // Finish of the file writeStream, extract rar archive
    }).on('finish', () => {
      console.log("Extracting archive.")
      extractArchive(out_path, out_dir)
    })
}

downloadMatch(test_getMatch, test_getMatchMapStats)
