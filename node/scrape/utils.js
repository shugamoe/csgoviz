const { unrar } = require('unrar-promise')
const tpath = '/home/jcm/matches/2337272/archive.rar'

function extractArchive(path, target_dir){
  console.log("Extracting %s", path)
  unrar(path, target_dir)
    .catch(err => console.log(err))
} 

module.exports.extractArchive = extractArchive
extractArchive(tpath)
