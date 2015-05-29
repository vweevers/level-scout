var select   = require('../select')
  , test     = require('./util/test')

test('select', false, function(t) {
  t.plan(8)

  select({hoi: 'k', a: true}).on('data', function(data){
    t.deepEqual(data, {hoi: 2, a: 3}, 'auto values')
  }).end({k: 2, a: 3, b: 4})

  select({k: 'key'}, {kv: false}).on('data', function(data){
    t.deepEqual(data, {k: 1}, 'explicit values')
  }).end({key: 1, value: 2})

  select({hoi: 'k', a: true}).on('data', function(data){
    t.deepEqual(data, {key: 1, value: {hoi: 2, a: 3}}, 'auto kv')
  }).end({key: 1, value: {k: 2, a: 3, b: 4}})

  select(['value', {a: true}]).on('data', function(data){
    t.deepEqual(data, {a: 3}, 'auto kv wants k+v')
  }).end({value: {k: 2, a: 3, b: 4}})

  select({a: true}).on('data', function(data){
    t.deepEqual(data, {key: 1, value: {a: 3}, extra: 3}, 'auto kv allows k+v+..')
  }).end({key: 1, value: {k: 2, a: 3, b: 4}, extra: 3})

  select({hoi: 'k', a: true}, {kv: true}).on('data', function(data){
    t.deepEqual(data, {key: 1, value: {hoi: 2, a: 3}}, 'explicit kv')
  }).end({key: 1, value: {k: 2, a: 3, b: 4}})

  select([{hoi: 'k', a: true}]).on('data', function(data){
    t.deepEqual(data, {hoi: 2, a: 3}, 'pass array')
  }).end({k: 2, a: 3, b: 4})

  select({hoi: 'k', a: true}).on('data', function(data){
    t.fail('should ignore non-objects')
  }).end(3)

  select().on('data', function(data){
    t.deepEqual(data, {k: 6}, 'empty selector does nothing')
  }).end({k: 6})
})
