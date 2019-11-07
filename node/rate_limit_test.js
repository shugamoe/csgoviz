const {RateLimiterMemory, RateLimiterQueue} = require('rate-limiter-flexible');
const fetch = require('node-fetch');

const limiterFlexible = new RateLimiterMemory({
    points: 1,
    duration: 2,
});

const limiterQueue = new RateLimiterQueue(limiterFlexible, {
    maxQueueSize: 100,
});

function sleep(milliSeconds) {
  var startTime = new Date().getTime();
  while (new Date().getTime() < startTime + milliSeconds);
}

for(let i = 0; i < 200; i++) {
    console.log("'i' is: %d", i)
    limiterQueue
      .removeTokens(1)
      .then((thing) => {
        console.log(typeof(thing))
        console.log(thing)
        fetch('https://github.com/animir/node-rate-limiter-flexible')
          .then(() => {
            console.log(Date.now())
          })
          .catch(err => console.error(err))
      })
      .catch(() => {
        console.log('queue is full')
            })
      // limiterQueue
      // .getTokensRemaining((thing) => {console.log(thing)})
        // .then((tokens_left) => {
          // console.log("hi %d", tokens_left)
          // sleep(1000)
        // })
}
