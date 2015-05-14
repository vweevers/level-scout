# level-scout

**ltgt syntax + bytewise encoded indexes + stream filters + query planner = pretty awesome search capabilities. A search will use a range query on the most optimal index, even intersect indexes if possible, or do a full scan.**

[![npm status](http://img.shields.io/npm/v/level-scout.svg?style=flat-square)](https://www.npmjs.org/package/level-scout) [![Travis build status](https://img.shields.io/travis/vweevers/level-scout.svg?style=flat-square&label=travis)](http://travis-ci.org/vweevers/level-scout) [![AppVeyor build status](https://img.shields.io/appveyor/ci/vweevers/level-scout.svg?style=flat-square&label=appveyor)](https://ci.appveyor.com/project/vweevers/level-scout) [![Dependency status](https://img.shields.io/david/vweevers/level-scout.svg?style=flat-square)](https://david-dm.org/vweevers/level-scout)

As an example, suppose you have a compound index on the `x` and `y` properties of your entities, resulting in index keys in the form of `[x, y, entity key]`. If you search for `x: 20, y: { gte: 5 }`, scout combines those predicates to a key range like `gte: [20, 5], lte: [20, undefined]`. But if you search for `x: { gte: 5 }, y: 20`, scout produces a ranged stream for `x` and filters that by `y`. Basically, scout can combine zero or more equality predicates with zero or one non-equality predicates, in the order of the index properties (so a compound "x, y" index is not the same as a "y, x" index). And maybe more in the future, if something like a "skip scan" is implemented.

This is experimental. API is unstable, documentation missing, terminology possibly garbled. Requires sublevel and leveldown (there are some unresolved issues with other backends like memdown).

## Quick overview

```js
var index  = require('level-scout/index')
    search = require('level-scout/search')
    select = require('level-scout/select')
    filter = require('level-scout/filter')

var db = ..

index(db, 'age')            // Single property index
index(db, 'owner.lastname') // Nested property
index(db, ['a', 'b', 'c'])  // Compound index

// Compound index with custom mapper. You
// can now search for `sum` even though it's
// not a property of the entity. Function
// is used for both indexing and filtering.
index(db, ['a', 'sum'], function(key, entity){
  return [entity.a, entity.a + entity.b]
})

// Insert some data
db.batch(..)

// Would select the "a, sum" index as access
// path, because those combined predicates are
// more selective than "age" - and "color" is not
// indexed.
var stream = search(db, {
  a: 45,
  sum: { gte: 45, lt: 60 },
  color: 'red',
  age: 300
})

// Get a subset of each entity
.pipe(select({the_age: 'age', color: true}))

// Filter some more (would yield no results)
.pipe(filter({ the_age: { lt: 100 } }))
```

Search with a callback:

```js
search(db, { year: 1988 }, function(err, results, plan){
  // `plan` contains debug info about the selected
  // access path and filters
})
```

## Setup

```js
var level    = require(..)
  , sublevel = require('level-sublevel/bytewise')
  , index    = require('level-scout/index')
  , search   = require('level-scout/search')

var db = sublevel(level(), { valueEncoding: 'json' })

index(db, ..)
search(db, ..)
```

Or attach the methods to your database:

```js
index.install(db)
search.install(db)

db.index('x')

db.put('key', {x: 10 }, function(){
  db.search({x: 10}, function(err, results){
    // ..
  })
})
```
