const fs = require('fs')
const path = require('path')
const { Transform } = require('stream');
const debug = require('debug')('gitdown')
const axios = require('axios')
const csv = require('csv')

const githubApi = {
  getLimits() {
    return axios.get('https://api.github.com/rate_limit').then(res => {
      debug('Current rate limits %o', res.data.rate)
      return res.data.rate
    })
  },
  getRepoUpdatedAt(url) {
    return axios.get(url)
       .then(res => res.data.updated_at)
       .catch(e => 'No info')
  }
}

const githubLimiter = opts => {
  let currLimit = opts.remaining

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, next) {
      debug('Got chunk in limiter')
      // For some reason, when github report remaining = 58 I can make only 57 requests,
      // last request is always failing with 'rate limit exceeded' :( , so I'm pausing before
      if (currLimit > 2) { // just pass if enough limit
        debug('Limit is %s', currLimit)
        currLimit--
        next(null, chunk)
        return
      }
      // else
      // if we are reaching limits than we have to get when they will reset
      githubApi.getLimits().then(({ remaining, reset }) => { // we can call GET /rate_limit w/o any limits
        this.pause()
        currLimit = remaining
        const wait = (reset - Math.round(Date.now() / 1000)) * 1000
        debug('Pause %s sec to restore rate-limit', wait / 1000)
        setTimeout(() => {
          this.resume()
          debug('Resume after timeout')
          next(null, chunk) // process chunk after timeout
        }, wait);
      })
    }
  })
}

const githubDownloader = new Transform({
  objectMode: true,
  transform(chunk, encoding, next) {
    debug('Got chunk in downloader')
    githubApi.getRepoUpdatedAt(chunk.repo_url).then(updatedAt => {
      debug('Fetched lastUpdate: %s', updatedAt)
      this.push({ ...chunk, updatedAt })
    })
    debug('Calling next')
    next() // we call next not in then to have parallel downloading
  }
})

githubApi.getLimits().then(limits => {
  fs.createReadStream(path.resolve(__dirname, 'github_dump_10k.csv'))
    .pipe(csv.parse({ columns: true }))
    .pipe(githubLimiter(limits))
    .pipe(githubDownloader)
    .pipe(csv.stringify({ header: true }))
    .pipe(process.stdout)
})
