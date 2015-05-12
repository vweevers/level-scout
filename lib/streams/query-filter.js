var through2     = require('through2').obj
  , range        = require('../util/range')
  , valuesOrKeys = require('../util/values-or-keys')
  , map          = require('../value-mapper/default')

module.exports = function(query, opts) {
  opts || (opts = {})

  var props = Object.keys(query)

  if (!props.length) { // Return a dead stream
    var stream = through2()
    setImmediate(stream.destroy.bind(stream, new Error('Empty query')))
    return stream
  }

  var filters = props.map(function(prop){
    return propertyFilter(prop, query[prop])
  })

  stream = through2(function(kv, _, next){
    for(var i=0, l=filters.length; i<l; i++)
      if (!filters[i](kv.key, kv.value)) return next()
    next(null, kv)
  })

  return valuesOrKeys(stream, opts)
}

function propertyFilter(prop, expr) {
  if (typeof expr !== 'object') expr = { eq: expr }

  var props = [prop]
    , match = range.matcher(range.normalize(1, expr))

  return function(key, entity) {
    var val = map(key, entity, props)
    return val != null && match(val)
  }
}
