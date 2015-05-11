var test = require('tape')
  , concat = require('concat-stream')
  , createDb = require('./util/create-db')

test('indexes', function(t) {
  var db = createDb(true)
  var xy = db.index(['x', 'y'])

  t.test('direct access', function(t){
    t.plan(1)
    db.put('bar', {x: 5, y: 20}, function(){
      xy.get([5, 20, 'bar'], function(err, value){
        t.equal(value, 'bar')
      })
    })
  })

  t.test('rebuild', function(t){
    var y = db.index('y')
    t.plan(2)
    db.put('foo', {x: 10, y: 6}, function(){
      y.rebuild(function() {
        y.get([6, 'foo'], function(err, value){
          t.equal(value, 'foo')
        })

        y.get([20, 'bar'], function(err, value){
          t.equal(value, 'bar')
        })
      })
    })
  })

  t.test('view stream', function(t){
    t.plan(2)
    db.batch([
      {key: 'a', value: {x: 100, y: 200}},
      {key: 'b', value: {x: 200, y: 300}},
      {key: 'c', value: {x: 300, y: 400}}
    ], function(){
      xy.viewStream({gt: 200}).on('data', function(data){
        t.equal(data.key, 'c', 'normalizes range')
      })
      xy.viewStream({gte: [200, 300]}).once('data', function(data){
        t.equal(data.key, 'b', 'sorted')
      })
    })
  })
})

test('index delete', function(t){
  t.plan(1)

  var db = createDb(true)
  var color = db.index('color')

  db.batch([
    {key: 'R', value: {color: 'red'}},
    {key: 'G', value: {color: 'green'}},
    {key: 'B', value: {color: 'blue'}}
  ], function(){

    // TODO: Uh. Actually test if index key is removed
    db.del('G', afterGC(color, function(){
      db.put('P', {color: 'pink'}, function(){
        color.viewStream({keys: true, values: false})
        .pipe(concat({encoding: 'object'}, function(keys){
          t.deepEqual(keys, [ 'B', 'P', 'R' ])
        }))
      })
    }))

  })

  // so we can see the debug output
  function afterGC(index, cb) {
    return function() {
      index.once('garbage', cb)
    }
  }
})

test('selectivity', function(t){
  t.plan(2)

  var db = createDb(true)
  var color = db.index('color')

  t.equal(color.selectivity(), null)

  db.batch([
    {key: 1, value: {color: 'red'}},
    {key: 2, value: {color: 'orange'}},
    {key: 3, value: {color: 'red'}}
  ], function(){
    t.equal(color.selectivity(), 2/3)
  })
})
