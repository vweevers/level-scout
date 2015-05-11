# level-scout

**Range syntax + bytewise encoded indexes + stream filters + query planner = pretty awesome search capabilities. A search will use a range query on the most optimal index, even intersect indexes if possible, or do a full scan.**

As an example, suppose you have a compound index on the `x` and `y` properties of your entities, resulting in index keys in the form of `[x, y, entity key]`. If you search for `x: 20, y: { gte: 5 }`, scout combines those predicates to a key range like `gte: [20, 5], lte: [20, undefined]`. But if you search for `x: { gte: 5 }, y: 20`, scout produces a ranged stream for `x` and filters that by `y`. Basically, scout can combine zero or more equality predicates with zero or one non-equality predicates, in the order of the index properties (so a compound "x, y" index is not the same as a "y, x" index). And maybe more in the future, if something like a "skip scan" is implemented.

This is experimental. API is unstable, documentation missing, terminology possibly garbled. Requires sublevel and leveldown (there are some unresolved issues with other backends like memdown).

## Quick overview

Index capabilities:

```js
index(db, 'age') // Single property index
index(db, 'owner.lastname') // Nested property

// Compound index (order matters)
index(db, ['a', 'b', 'c']) 

// Compound index with custom mapper. You
// can now search for `sum` even though it's
// not a property of the entity. Function
// is used for both indexing and filtering.
index(db, ['a', 'sum'], function(key, entity){
  return [entity.a, entity.a + entity.b]
})

// Stats (work in progress)
index(db, 'color').cardinality()

```

Search capabilities:

```js
// Returns a stream
search(db, { x: 3 })
search(db, { x: { eq: 3 }})
search(db, { x: { gt: 50, lt: 100 }, y: {lte: 250} )
search(db, { name: 'kangaroo' })
search(db, { name: { gt: 'kanga' } })

// But you can also pass a callback
search(db, { year: 1988 }, function(err, results, plan){
  // Note: `plan` contains debug info about the selected
  // access path and filters
})

```

## Setup

```js
var level    = require(..)
  , sublevel = require('level-sublevel/bytewise')
  , index    = require('level-scout').index
  , search   = require('level-scout').search

var db = sublevel(level(), { valueEncoding: 'json' })

index(db, 'x')
search(db, {x: 10})

```

Or attach the methods to your database:

```js
index.install(db)
search.install(db)

db.index('x')
db.search({x: 10})

```
