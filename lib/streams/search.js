var QueryPlan = require('../util/query-plan')
  , queryFilter = require('./query-filter')
  , viewStream = require('./view')
  , debug = require('debug')('level-scout')
  , util = require('util')
  , valuesOrKeys = require('../util/values-or-keys')
  , mapFilter = require('./map-filter')
  , intersect = require('sorted-intersect-stream')

// Search selects the most optimal index(es) and adds filters if necessary.
module.exports = function (db, query, opts) {
  explicitEq(query)

  if (opts && opts.indexes) {
    var indexes = opts.indexes

    if (Array.isArray(indexes)) {
      indexes = Object.create(null)
      opts.indexes.forEach(function(index){
        indexes[index.name] = index
      })
    }
  } else {
    indexes = db.indexes || Object.create(null)
  }

  // TODO: Maybe use lazypipe to construct plan + streams
  // TODO: Don't decode keys, use raw keyEncoding, decode after filtering
  // TODO: In viewStream, if entity does not exist, notify gc
  // TODO: Compute overhead (old index keys not deleted due to a process crash)
  //       while filtering and notify gc
  
  var plan = new QueryPlan(query, indexes)
    , paths = plan.accessPaths
    , debugPlan = []

  // TODO: move this to QueryPlan
  if (!paths.length) {
    var stream = db.createReadStream()
    debugPlan.push('Table scan')
  } else if (paths.length === 1) {
    stream = keyStream(paths[0]).pipe(viewStream(db))
    debugPlan.push(paths[0])
  } else {
    // We can do an index intersect when we have multiple
    // full-range equality predicates (because then, streams are key-ordered).
    if (plan.equiPaths.length > 1) {
      var merged = plan.equiPaths.reduce(function(acc, path){
        return acc ? intersect(acc, keyStream(path)) : keyStream(path)
      }, null)

      stream = merged.pipe(viewStream(db))
      debugPlan.push({intersect: plan.equiPaths})
    } else {
      // TODO: after the number of matched predicates, sort the 
      // indexes by cardinality.

      // First index should be most optimal.
      stream = keyStream(paths[0]).pipe(viewStream(db))
      debugPlan.push(paths[0])
    }
  }

  // Filter by indexed predicates (also to catch old indexKeys)
  plan.filters.forEach(function(filter){
    stream = stream.pipe(mapFilter(filter.map, filter.range, filter.match))
  })

  // Filter by unindexed predicates
  if (plan.extraneous.length) {
    var restQuery = Object.create(null)
    
    plan.extraneous.forEach(function(property){
      restQuery[property] = query[property]
    })

    debugPlan.push({filter: restQuery})
    stream = stream.pipe(queryFilter(restQuery))
  }

  stream = valuesOrKeys(stream, opts)
  stream.plan = debugPlan

  debug(util.inspect({plan: debugPlan}, {depth: 4}))
  return stream
}

function keyStream(path) {
  return path.index.createValueStream(path.range)
}

function explicitEq(query) {
  Object.keys(query).forEach(function(p){
    if (typeof query[p] !== 'object') query[p] = { eq: query[p] }
  })
}
