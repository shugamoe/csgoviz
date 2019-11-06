const { HLTV } = require('hltv')

var beef = HLTV.getMatchesStats({startDate: '2017-07-10', endDate: '2017-07-10'}).then((res) => {
  console.log(Object.keys(res))
  console.log(res[1])
})
