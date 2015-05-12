var test = require('tape')
  , createDb = require('./util/create-db')

test('gc queue is injected into batch', function(t){
  t.plan(2)

  var db = createDb(true)
  var color = db.index('color')

  db.put('G', {color: 'green'}, function(){
    color.get(['green', 'G'], function(err, key){
      t.equal(key, 'G', 'has index key')

      db.del('G')
      db.once('gc', function(){
        db.put('P', {color: 'pink'}, function(){
          color.get(['green', 'G'], function(err, key){
            t.ok(err, 'index key removed')
          })
        })
      })
    })
  })
})

test('gc queue is flushed after delay', function(t){
  t.plan(2)

  var db = createDb(true)
  var color = db.index('color', { gc: { delay: 10 }})

  db.batch([
    {key: 'R', value: {color: 'red'}},
    {key: 'G', value: {color: 'green'}},
    {key: 'B', value: {color: 'blue'}}
  ], function(){
    color.get(['green', 'G'], function(err, key){
      t.equal(key, 'G', 'has index key')

      db.del('G')
      db.once('gc', function(){
        setTimeout(function(){
          color.get(['green', 'G'], function(err, key){
            t.ok(err, 'index key removed')
          })
        }, 300)
      })
    })
  })
})

test('gc queue is flushed if full', function(t){
  t.plan(2)

  var db = createDb(true)
  var color = db.index('color', { gc: { size: 1, delay: 60000 }})

  db.batch([
    {key: 'R', value: {color: 'red'}},
    {key: 'G', value: {color: 'green'}},
    {key: 'B', value: {color: 'blue'}}
  ], function(){
    color.get(['green', 'G'], function(err, key){
      t.equal(key, 'G', 'has index key')

      db.del('G')
      db.once('gc', function(){
        setTimeout(function(){
          color.get(['green', 'G'], function(err, key){
            t.ok(err, 'index key removed')
          })
        }, 300)
      })
    })
  })
})

test('queued gc op is canceled by new write', function(t){
  t.plan(3)

  var db = createDb(true)
  var color = db.index('color')

  db.put('G', {color: 'green'}, function(){
    color.get(['green', 'G'], function(err, key){
      t.equal(key, 'G', 'has index key')

      db.del('G')
      db.once('gc', function(){
        db.put('G', {color: 'pink'}, function(){
          color.get(['green', 'G'], function(err, key){
            t.notOk(err, 'old index key not removed')
          })
          color.get(['pink', 'G'], function(err, key){
            t.notOk(err, 'has new index key')
          })
        })
      })
    })
  })
})

test('a new write is requeued if gc is writing', function(t){
  t.plan(4)

  var db = createDb(true)
  var color = db.index('color')

  var batch = db.batch
  var delayedBatch = function() {
    var args = [].slice.apply(arguments)
    setTimeout(batch.apply.bind(batch, db, args), 300)
  }

  db.put('G', {color: 'green'}, function(){
    color.get(['green', 'G'], function(err, key){
      t.equal(key, 'G', 'has key')

      db.del('G')
      db.once('gc', function(){
        // Simulate delayed flush
        db.batch = delayedBatch

        db.gc.flush(function(){
          color.get(['green', 'G'], function(err, key){
            t.ok(err, 'old key removed')
          })

          color.get(['pink', 'G'], function(err, key){
            t.ok(err, 'new key not yet flushed')
            db.gc.flush(function(){
              color.get(['pink', 'G'], function(err, key){
                t.notOk(err, 'new key flushed')
              })
            })
          })
        })

        db.batch = batch

        // Write new value before flush is finished
        db.gc.cancel('G', [
          { prefix: color.db, key: ['pink', 'G'], value: null }
        ])
      })
    })
  })
})
