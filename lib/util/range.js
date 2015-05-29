var lowerBound = null
  , upperBound = undefined

exports.matcher = function(expr) {
  if ('eq' in expr) return function(a, b) {
    return compare(a, b) === 0
  }.bind(null, expr.eq)

  if ('gt' in expr)
    var gt = compare.bind(null, expr.gt), min = -1
  else if ('gte' in expr)
    gt = compare.bind(null, expr.gte), min = 0

  if ('lt' in expr)
    var lt = compare.bind(null, expr.lt), max = 1
  else if ('lte' in expr)
    lt = compare.bind(null, expr.lte), max = 0

  return function(b) {
    return (!gt || gt(b) <= min) && (!lt || lt(b) >= max)
  }
}

function compare (a, b) {
  if (a === lowerBound && b !== lowerBound) return -1
  if (a === upperBound && b !== upperBound) return  1
  if (b === lowerBound && a !== lowerBound) return  1
  if (b === upperBound && a !== upperBound) return -1
  if (a === lowerBound || a === upperBound) return  0

  if(isArrayLike(a) && isArrayLike(b)) {
    var la = a.length, lb = b.length, c

    for(var i=0; i<la && i<lb; i++) {
      if (c = compare(a[i], b[i])) return c
    }

    a = la; b = lb
  }

  return a < b ? -1 : a > b ? 1 : 0
}

function isArrayLike (a) {
  return Array.isArray(a) || Buffer.isBuffer(a)
}

exports.exprToRange = function (expressions, meta) {
  var range = {}
    , key = [], equirange = true

  expressions.forEach(function(expr){
    if ('eq' in expr) return key.push(expr.eq)

    if (!equirange)
      throw new Error('Supports only one non-equi predicate')

    equirange = false

    if ('lt' in expr) range.lt  = key.concat(expr.lt)
    else if ('lte' in expr && !('lt' in range)) range.lte = key.concat(expr.lte)
    
    if ('gt' in expr) range.gt  = key.concat(expr.gt)
    else if ('gte' in expr && !('gt' in range)) range.gte = key.concat(expr.gte)
  })

  if (equirange) range.eq = key
  else if (key.length) { // non-equi needs counterpart
    for(var op in range) {
      var oppo = (op[0] == 'l' ? 'g' : 'l') + op.slice(1)
        , alt = op[2] == 'e' ? oppo.slice(0,2) : oppo + 'e'

      if (!(oppo in range) && !(alt in range))
        range[oppo] = key.slice()
    }
  }

  return meta ? [range, equirange] : range
}

exports.normalize = function normalize(length, opts) {
  var normal = {}

  if ('eq' in opts) {
    // Fix `eq` if it is an incomplete range, to gte+lte
    var key = opts.eq
    if (!Array.isArray(key)) key = [key]

    if (key.length!==length) {
      return normalize(length, { gte: key.slice(), lte: key.slice() })
    } else {
      normal.eq = key
      return normal
    }
  }

  function fill(val, fill) {
    var a = Array.isArray(val) ? val.slice() : [val]
    while(a.length < length) a.push(fill)
    return a
  }

  if ('gt'  in opts) normal.gt = fill(opts.gt, upperBound)
  else if ('gte' in opts) normal.gte = fill(opts.gte, lowerBound)

  if ('lt'  in opts) normal.lt = fill(opts.lt, lowerBound)
  else if ('lte' in opts) normal.lte = fill(opts.lte, upperBound)

  return normal
}
