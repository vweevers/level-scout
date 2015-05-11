var test = require('tape')
  , queryFilter = require('../').filter
  , concat = require('concat-stream')
  , createDb = require('./util/create-db')

test('query filter (does not use indexes)', function(t) {
  var db = createDb()

  t.plan(15)

  db.batch([
    { key: 'readme',  value: {type: 'text', size: 30 }},
    { key: 'logo',    value: {type: 'png',  size: 1000, author: { name: 'mary'} }},
    { key: 'license', value: {type: 'text', size: 10 }}
  ], function(err) {
    q({size: 10}, ['license'], 'default is eq')
    q({size: 30}, ['readme'], 'default is eq')

    q({size: {eq: 30}}, ['readme'], 'eq')
    q({size: {gte: 30}}, ['logo', 'readme'], 'gte')
    q({size: {gt: 30}}, ['logo'], 'gt')
    q({size: {lt: 10}}, [], 'lt')
    q({size: {lte: 10}}, ['license'], 'lte')
    q({size: {lte: 1000, gt: 10}}, ['logo', 'readme'], 'lte + gt')
    q({size: {eq: 10, gt: 10}}, ['license'], 'eq takes precedence')

    q({size: {gt: 0}}, ['license', 'logo', 'readme'], 'gt 0')
    q({size: {gt: null}}, ['license', 'logo', 'readme'], 'gt null')

    q({type: 'text'}, ['license', 'readme'])
    q({type: 'text', size: 30}, ['readme'], 'multiple props')
    q({type: 'text', size: {gte: 30}}, ['readme'], 'multiple props')

    q({'author.name': 'mary'}, ['logo'], 'nested property')
  })

  function q(query, expectedKeys, msg) {
    // TODO: { keys: true, values: false } opts doesn't work.
    db.createReadStream().pipe(queryFilter(query)).pipe(concat(function(items){
      var keys = items.map(function(item){
        return item.key
      })
      keys.sort()
      t.deepEqual(keys, expectedKeys, msg)
    }))
  }
})
