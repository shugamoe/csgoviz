const { unrar, list } = require('unrar-promise')
var Promise = require('bluebird')
const tarchPath = '/home/jcm/matches/2337272/archive.rar'

function extractArchive (archPath, targetDir, matchID) {
  return new Promise(async (resolve, reject) => {
    console.log(`|${matchID} Extracting . . .`)
    try {
      await unrar(archPath, targetDir)
      resolve(list(archPath))
    } catch (err) {
      console.dir(err)
      reject(err)
    }
  })
}

module.exports.extractArchive = extractArchive
// extractArchive(tarchPath)
