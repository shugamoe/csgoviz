const { unrar } = require('unrar-promise')
var Promise = require('bluebird')
const tpath = '/home/jcm/matches/2337272/archive.rar'

function extractArchive(path, target_dir){
  return new Promise(async (resolve, reject) => {
    console.log("Extracting %s", path)
    try {
      const thing = await unrar(path, target_dir)
      resolve()
    } catch(err) {
      console.dir(err)
      resolve()
    }
  })
} 

module.exports.extractArchive = extractArchive
// extractArchive(tpath)
