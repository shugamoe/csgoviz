const { unrar, list } = require('unrar-promise')
var Promise = require('bluebird')
const tarchPath = '/home/jcm/matches/2337272/archive.rar'

function extractArchive(archPath, target_dir){
  return new Promise(async (resolve, reject) => {
    console.log("Extracting %s", archPath)
    try {
      await unrar(archPath, target_dir)
      resolve(list(archPath))
    } catch(err) {
      console.dir(err)
      reject(undefined)
    }
  })
} 

module.exports.extractArchive = extractArchive
// extractArchive(tarchPath)
