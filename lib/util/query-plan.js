var debug       = require('debug')('level-scout')
  , exprToRange = require('../util/range').exprToRange
  , createTree  = require('./property-tree').create

module.exports = QueryPlan

function QueryPlan (query, indexes, tree) {
  this.query = query
  this.indexes = indexes
  this.indexed = []
  this.tree = tree || createTree(indexes)

  // Indexed properties grouped by operator: eq or other
  this.equi = []
  this.nonEqui = []

  var properties = Object.keys(query)

  for(var name in indexes) {
    var p = indexes[name].properties
    
    for(var j=0, jl=p.length; j<jl; j++) {
      if (properties.indexOf(p[j])>=0 && this.indexed.indexOf(p[j])<0) {
        this.indexed.push(p[j])
        ;('eq' in this.query[p[j]] ? this.equi : this.nonEqui).push(p[j])
      }
    }
  }

  this.extraneous = properties.filter(notContainedIn, this.indexed)
  
  this.selectAccessPaths()
  this.selectFilters()
}

// Select indexes for scanning
QueryPlan.prototype.selectAccessPaths = function () {
  this.equiPaths = []
  this.accessPaths = this.selectCandidates(true, this.equiPaths)

  debug({query: this.query})
  debug({winners: this.accessPaths})
}

// Select indexes for filtering
QueryPlan.prototype.selectFilters = function () {
  this.filters = this.selectCandidates(false)
}

// Select indexes until all predicates are represented
// TODO: for scanning, we'll likely use only one index.
// Once I have that part figured out, skip this and just
// use the first candidate.
QueryPlan.prototype.selectCandidates = function (contiguous, accEquality) {
  var remaining = this.indexed.slice()
    , paths = [], had = Object.create(null)
    , candidates = []

  // Find candidates
  this.traverse(contiguous, this.tree, candidates)

  // Sort so that the first candidate has the most
  // matched and least unmatched properties
  // TODO: include cardinality.
  candidates.sort(optimalFirst)

  candidateLoop:
  for(var i=0, l=candidates.length; i<l && remaining.length; i++) {
    var candidate = candidates[i]

    if (had[candidate.name] && contiguous) continue
    else had[candidate.name] = true

    for(var j=0, jl=candidate.match.length; j<jl; j++) {
      var property = candidate.match[j]
      if (remaining.indexOf(property) < 0) continue candidateLoop
    }

    remaining = remaining.filter(notContainedIn, candidate.match)

    var predicates = candidate.match.map(from, this.query)
      , a = exprToRange(predicates, true)

    var path = {
      index: candidate.index,
      map:   candidate.map,
      match: candidate.match,
      range: a[0]
    }

    if (accEquality != null && a[1] && !candidate.unmatched)
      accEquality.push(path)

    paths.push(path)
  }

  return paths
}

// Select index candidates by matching their properties
// to the query. We can combine zero or more equality
// predicates with zero or one non-equi predicates. If
// contiguous is false, index properties may be skipped.
QueryPlan.prototype.traverse = function (contiguous, node, acc, parent) {
  var set = parent || [], subset

  function add(node, set) {
    if (node.__index) {
      var index = node.__index
      return acc.push({
        index: index, name: index.name, map: index.map,
        match: set, unmatched: index.properties.length - set.length
      })
    }

    for(var n in node) if (n!=='__index' && add(node[n], set))
      return true
  }

  for(var i=0, l=this.equi.length; i<l; i++) {
    var prop = this.equi[i]
    if (!(prop in node)) continue

    add(node[prop], subset = set.concat(prop))
    this.traverse(contiguous, node[prop], acc, subset)

    if (!contiguous) {
      this.traverse(contiguous, node[prop], acc, parent)
      this.traverse(contiguous, node[prop], acc)
    }
  }

  for(i=0, l=this.nonEqui.length; i<l; i++) {
    prop = this.nonEqui[i]
    if (!(prop in node)) continue

    add(node[prop], set.concat(prop))

    if (!contiguous) {
      this.traverse(contiguous, node[prop], acc, parent)
      this.traverse(contiguous, node[prop], acc)
    }
  }
}

function optimalFirst(a, b){
  var matched = b.match.length - a.match.length
  if (matched !== 0) return matched

  var as = a.index.selectivity(), bs = b.index.selectivity(), cmp = bs - as
  if (cmp !== 0 && cmp !== as + bs) return cmp

  return (a.unmatched - b.unmatched) || (a.name - b.name)
}

function from(item){
  return this[item]
}

function notContainedIn(item) {
  return this.indexOf(item) < 0
}
