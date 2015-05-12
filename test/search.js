var index    = require('../index')
  , search   = require('../search')
  , test     = require('tape')
  , concat   = require('concat-stream')
  , through2 = require('through2').obj
  , createDb = require('./util/create-db')

test('custom index map function', function(t) {
  var db = createDb()

  t.test('single property', function(t){
    t.plan(2)

    var sdb = db.sublevel('one')

    index(sdb, 'sum', function(key, value){
      return [ value.a + value.b ]
    })

    run(sdb, function(keys, plan){
      t.deepEqual(keys, ['four', 'one'])
      t.deepEqual(plan, [ 
        { index: 'sum', range: { gte: [ 45 ] } }, 
        { filter: { a: { lt: 15 } } }
      ]);
    })
  })

  t.test('two properties', function(t){
    t.plan(2)

    var sdb = db.sublevel('two')

    index(sdb, ['a', 'sum'], function(key, value){
      return [ value.a, value.a + value.b ]
    })

    run(sdb, function(keys, plan){
      t.deepEqual(keys, ['four', 'one'])
      t.deepEqual(plan, [ 
        { index: 'a,sum', range: { lt: [ 15 ] } }, 
        // { mapFilter: 'a,sum', subset: ['sum'], range: { gte: [ 45 ] } }
      ]);
    })
  })

  function run(db, cb) {
    var data = [
      { key: 'one',   value: { a: 10, b: 35 } },
      { key: 'two',   value: { a: 10, b: 10 } }, // `sum` is too small
      { key: 'three', value: { a: 20, b: 10 } }, // `a` is too big
      { key: 'four',  value: { a: 0,  b: 45 } }
    ]

    db.batch(data, function(err){
      var stream = search(db, {
        sum: { gte: 45 },
        a:   {  lt: 15 },
      }, { values: false })

      stream.pipe(concat({encoding: 'object'}, function(keys){
        cb(keys, flat(stream.plan))
      }))
    })
  }
})

test('search with indexes', function(t){
  t.plan(13)

  var db = createDb(true, true)
  var xy = db.index(['x', 'y'])
  var x = db.index('x')
  var y = db.index('y')

  // TODO: separate tests
  db.put('bar', {x: 5, y: 20}, function(){
    // test compound index x+y
    var stream = db.search({
      x: 5,
      y: { gte: 20 }
    })

    stream.on('data', function(data){ t.equal(data.key, 'bar', 'stream1 data ok') })
    t.deepEqual(flat(stream.plan), [
      {index: 'x,y', range: {gte: [5, 20], lte: [5] }}
    ], 'compound x+y index')

    // test two indexes x, y
    var stream2 = db.search({
      x: { eq: 5 },
      y: { gte: 20 }
    }, {indexes: [y, x]})

    stream2.on('data', function(data){ t.equal(data.key, 'bar', 'stream2 data ok') })
    t.deepEqual(flat(stream2.plan), [
      {index: 'x', range: {  eq: [ 5 ] }}
    ], 'x (eq) and y (gte) index ')

    // test single index y
    var stream2b = db.search({
      y: { gte: 20 }
    }, {indexes: [x, y]})

    stream2b.on('data', function(data){ t.equal(data.key, 'bar', 'stream2b data ok') })
    t.deepEqual(flat(stream2b.plan), [{index: 'y', range: { gte: [ 20 ] }}], 'y index')

    // test single index x
    var stream3 = db.search({
      x: { lte: 5 }
    }, {indexes: [x, y]})

    stream3.on('data', function(data){ t.equal(data.key, 'bar', 'stream3 data ok') })
    t.deepEqual(flat(stream3.plan), [{index: 'x', range: { lte: [ 5 ] }}], 'x index')

    // test single index x
    var stream3b = db.search({
      x: { lt: 50 },
      y: { gt: 2  }
    }, {indexes: [x]})

    stream3b.on('data', function(data){ t.equal(data.key, 'bar', 'stream3b data ok') })
    t.deepEqual(flat(stream3b.plan), [
      { index: 'x', range: { lt: [ 50 ] }},
      { filter: { y: { gt: 2 } } }
    ], 'x index + filter')

    // test two indexes x, y
    var stream4 = db.search({
      x: 5,
      y: 20
    }, {indexes: [x, y]})

    t.deepEqual(flat(stream4.plan), [
      {intersect: [
        {index: 'x', range: { eq: [ 5 ] }},
        {index: 'y', range: { eq: [ 20 ] }},
      ]}
    ], 'x (eq) and y (eq) indexes')

    stream4.pipe(concat(function(items){
      t.deepEqual(items, [{key: 'bar', value: {x:5, y:20}}], 'stream4 data ok')
    }))

    db.search({
      x: 5,
      y: 20
    }, {indexes: [x, y]}, function(err, items){
      t.deepEqual(items, [{key: 'bar', value: {x:5, y:20}}], 'using a callback')
    })
  })
})

test('table scan', function(t){
  t.plan(4)

  var db = createDb()

  function q(query, expected, msg) {
    search(db, query, { keys: true, values: false }, function(err, keys){
      t.deepEqual(keys, expected, msg)
    })
  }

  db.batch([
    {key: 1, value: { seq: 'a' }},
    {key: 2, value: { seq: 'aa' }},
    {key: 3, value: { seq: 'b' }}
  ], function(){
    q({seq: 'aa'}, [2], 'implicit eq')
    q({seq: {lt: undefined}}, [1,2,3], 'lt undefined')
    q({seq: {gt: null}}, [1,2,3], 'gt null')
    q({seq: {gt: 'a', lt: 'b'}}, [2], 'gt + lt')
  })
})

test('intersect two indexes', function(t){
  t.plan(2)
  var db = createDb()

  index(db, 'x')
  index(db, 'y')

  db.batch([
    {key: 1, value: { x: 9, y: 2 }},
    {key: 2, value: { x: 7, y: 5 }},
    {key: 3, value: { x: 9, y: 2 }},
  ], function(){
    search(db, {
      x: 9,
      y: 2,
    }, {keys: true, values: false}, function(err, keys, plan){
      t.deepEqual(keys, [1,3], 'data ok')
      t.deepEqual(flat(plan), [ { intersect: [ 
        { index: 'x', range: { eq: [9] } }, 
        { index: 'y', range: { eq: [2] } } ] 
      }], 'plan ok')
    })
  })
})

test('intersect two compound indexes', function(t){
  t.plan(2)
  var db = createDb()

  index(db, ['a', 'b'])
  index(db, ['c', 'd'])

  db.batch([
    {key: 1, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 2, value: { a: 7, b: 5, c: 2, d: 4 }},
    {key: 3, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 4, value: { a: 9, b: 2, c: 2, d: 5 }},
    {key: 5, value: { a: 9, b: 2, c: 3, d: 4 }},
  ], function(){
    search(db, {
      a: 9, b: 2, c: 2, d: 4
    }, {keys: true, values: false}, function(err, keys, plan){
      t.deepEqual(keys, [1,3], 'data ok')
      t.deepEqual(flat(plan), [ { intersect: [ 
        { index: 'a,b', range: { eq: [9,2] } }, 
        { index: 'c,d', range: { eq: [2,4] } } ] 
      }], 'plan ok')
    })
  })
})

test('will not intersect partial range on compound indexes', function(t){
  t.plan(2)
  var db = createDb()

  index(db, ['a', 'b'])
  index(db, ['c', 'd'])

  db.batch([
    {key: 1, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 2, value: { a: 7, b: 5, c: 2, d: 4 }},
    {key: 3, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 4, value: { a: 9, b: 2, c: 2, d: 5 }},
    {key: 5, value: { a: 9, b: 2, c: 3, d: 4 }},
  ], function(){
    search(db, {
      a: 9, b: 2, c: 2
    }, {keys: true, values: false}, function(err, keys, plan){
      t.deepEqual(keys, [1,3,4], 'data ok')
      t.deepEqual(flat(plan), [ { index: 'a,b', range: { eq: [ 9, 2 ] } } ], 'plan ok')
    })
  })
})

test('will not intersect non-equi range on compound indexes', function(t){
  t.plan(2)
  var db = createDb()

  index(db, ['c', 'd'])
  index(db, ['a', 'b'])

  db.batch([
    {key: 1, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 2, value: { a: 7, b: 5, c: 2, d: 4 }},
    {key: 3, value: { a: 9, b: 2, c: 2, d: 4 }},
    {key: 4, value: { a: 9, b: 2, c: 2, d: 5 }},
    {key: 5, value: { a: 9, b: 2, c: 3, d: 4 }},
  ], function(){
    search(db, {
      a: 9, b: 2, c: 2, d: { lte: 4, gt: 2 }
    }, {keys: true, values: false}, function(err, keys, plan){
      t.deepEqual(keys, [1,3], 'data ok')
      t.deepEqual(flat(plan), [ { index: 'c,d', range: { gt: [ 2, 2 ], lte: [ 2, 4 ] } } ], 'plan ok')
    })
  })
})

test('lte + gt predicates on compound index', function(t){
  t.plan(2)
  var db = createDb()

  index(db, ['c', 'd'])

  db.batch([
    {key: 1, value: { c: 0, d: 4 }},
    {key: 2, value: { c: 1, d: 4 }},
    {key: 3, value: { c: 0, d: 4 }},
    {key: 4, value: { c: 0, d: 5 }},
    {key: 5, value: { c: 0, d: 1 }},
  ], function(){
    search(db, {
      c: 0, d: { lte: 4, gt: 2 }
    }, {keys: true, values: false}, function(err, keys, plan){
      t.deepEqual(keys, [1,3], 'data ok')
      t.deepEqual(flat(plan), [ 
        { index: 'c,d', range: { gt: [ 0, 2 ], lte: [ 0, 4 ] } }
      ], 'plan ok')
    })
  })
})

test('non objects', function(t){
  t.plan(3)

  var db = createDb()
  var suffix = index(db, 'suffix', function(key, value){
    return [ value.split('.')[1] ]
  })

  function q(query, expected, msg) {
    search(db, query, { keys: true, values: false }, function(err, keys){
      t.deepEqual(keys, expected, msg)
    })
  }

  db.batch([
    {key: 1, value: '1.a'  },
    {key: 2, value: '2.aa' },
    {key: 3, value: '3.ab' },
    {key: 4, value: '4.ba' }
  ], function(){
    q({ suffix: { gt: 'a0'} },  [2,3,4],   'gt')
    q({ suffix: 'ba' },         [4],       'implicit eq')
    q({ suffix: { gt: null } }, [1,2,3,4], 'gt null')
  })
})

test('favor high selectivity', function(t){
  t.test('x vs y', function(t){
    t.plan(4)

    var db = createDb()
    var x = index(db, 'x'), y = index(db, 'y')

    db.batch([
      { key: 1, value: { x: 0, y: 0 } },
      { key: 2, value: { x: 1, y: 1 } },
      { key: 3, value: { x: 0, y: 2 } },
      { key: 4, value: { x: 1, y: 3 } },
      { key: 5, value: { x: 0, y: 4 } },
    ], function(){
      t.equal(x.selectivity(), 2/5, 'x selectivity: 0.2')
      t.equal(y.selectivity(), 5/5, 'y selectivity: 1.0')

      search(db, {
        x: { gt: 0 }, y: { gt: 0 }
      }, {keys: true, values: false}, function(err, items, plan){
        t.deepEqual(items, [2,4], 'data ok')
        t.deepEqual(flat(plan), [ { index: 'y', range: { gt: [ 0 ] } } ], 'y won')
      })
    })
  })

  t.test('y vs x', function(t){
    t.plan(4)

    var db = createDb()
    var x = index(db, 'x'), y = index(db, 'y')

    db.batch([
      { key: 1, value: { x: 0, y: 0 } },
      { key: 2, value: { x: 1, y: 1 } },
      { key: 3, value: { x: 2, y: 0 } },
      { key: 4, value: { x: 3, y: 1 } },
      { key: 5, value: { x: 4, y: 0 } },
    ], function(){
      t.equal(x.selectivity(), 5/5, 'x selectivity: 1.0')
      t.equal(y.selectivity(), 2/5, 'y selectivity: 0.2')

      search(db, {
        x: { gt: 0 }, y: { gt: 0 }
      }, {keys: true, values: false}, function(err, items, plan){
        t.deepEqual(items, [2,4], 'data ok')
        t.deepEqual(flat(plan), [ { index: 'x', range: { gt: [ 0 ] } } ], 'x won')
      })
    })
  })
})

test.skip('pseudocode - merge join', function(t){
  // SELECT * FROM employee e, company c WHERE e.company_id = c.id

  var employeesByCompany = select(employees, {order: 'company_id'})
    , companiesById = select(companies, {order: 'id'})

  return intersect( employeesByCompany, companiesById, function(employee){
    return employee.company_id
  }, function(company){
    return company.id
  })
})

test.skip('pseudocode - function as query predicate', function(t){
  search(db, {
    color: function(value) {
      return value == 'red'
    }
  })
})

function flat(plan) {
  return plan.map(function(item){
    if (Array.isArray(item)) return flat(item)
    if (item.index) item.index = item.index.name
    if (item.intersect) {
      item.intersect = flat(item.intersect)
      item.intersect.sort(function(a, b){
        if (a.index===b.index) return 0
        return a.index < b.index ? -1 : 1
      })
    }
    
    delete item.map
    delete item.match
    
    return item
  })
}
