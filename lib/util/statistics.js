var schedule    = require('level-schedule')
  , through2    = require('through2').obj
  , sigmund     = require('sigmund')
  , debug       = require('debug')('level-scout')
  , HyperLogLog = require('hyperloglog32')
  , bytespace   = require('bytespace')

// Update statistics one index at a time
module.exports = function(db, indexes) {
  var interval = 300 // seconds
  
  // TODO: key encoding should just be utf8?
  var sub = bytespace(db, 'scout-stat-jobs', { 
    valueEncoding: 'utf8',
    keyEncoding: bytespace.bytewise
  })

  var jobs = schedule(sub)

  jobs.job('cardinality', function (payload, done) {
    var names = Object.keys(indexes), job = this

    function next() {
      if (!names.length) return end()

      var name = names.pop()
      if (!indexes[name]) return next()

      updateIndex(indexes[name], next)
    }

    function end() {
      job.run('cardinality', Date.now() + interval)
      done()
    }

    next()
  })

  jobs.run('cardinality', Date.now() + interval);
}

function updateIndex(index, done) {
  var h = HyperLogLog(12) // Standard error is 1.625%
  var length = 0 // Approximate number of total rows

  function end(err) {
    if (err) debug(err)
    done && done()
    done = null
  }

  index.createKeyStream().pipe(through2(function(key, _, next){
    h.add(sigmund(key.slice(0,-1)))
    length++
    next()
  })).on('end', function() {

    // TODO: maybe save all stats in one place
    index.saveStatistics({
      cardinality: h.count(),
      length: length
    }, end)

  }).on('error', end)
}
