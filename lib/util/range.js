var bytewise = require('bytewise')

var MIN = null
var MAX = undefined

exports.matcher = function(expr) {
  var eq  = 'eq'  in expr ? bytewise.encode(expr.eq)  : null
    , gt  = 'gt'  in expr ? bytewise.encode(expr.gt)  : null
    , lt  = 'lt'  in expr ? bytewise.encode(expr.lt)  : null
    , gte = 'gte' in expr ? bytewise.encode(expr.gte) : null
    , lte = 'lte' in expr ? bytewise.encode(expr.lte) : null

  return function(array) {
    var b = bytewise.encode(array)

    if (eq  !== null && bytewise.compare(eq, b) !=0) return false
    if (gt  !== null && bytewise.compare(gt, b) >=0) return false
    if (lt  !== null && bytewise.compare(lt, b) <=0) return false
    if (gte !== null && bytewise.compare(gte, b) >0) return false
    if (lte !== null && bytewise.compare(lte, b) <0) return false

    return true
  }
}

exports.exprToRange = function (expressions, meta) {
  var range = Object.create(null)
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
    while(a.length<length) a.push(fill)
    return a
  }

  if ('gt'  in opts) normal.gt  = fill(opts.gt,  MAX)
  if ('lt'  in opts) normal.lt  = fill(opts.lt,  MIN)
  if ('gte' in opts) normal.gte = fill(opts.gte, MIN)
  if ('lte' in opts) normal.lte = fill(opts.lte, MAX)

  return normal
}
