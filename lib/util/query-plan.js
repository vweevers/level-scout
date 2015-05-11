var permutationCombination = require('js-combinatorics').permutationCombination
  , debug = require('debug')('level-scout')
  , exprToRange = require('../util/range').exprToRange

module.exports = QueryPlan

function QueryPlan (query, indexes) {
  this.query = query
  this.indexes = indexes
  this.properties = Object.keys(query)
  this.names = Object.keys(indexes)
  this.indexed = []

  // Indexed properties grouped by operator: eq or other
  this.equi = []
  this.nonEqui = []

  for(var i=0, il=this.names.length; i<il; i++) {
    var p = indexes[this.names[i]].properties
    
    for(var j=0, jl=p.length; j<jl; j++) {
      if (this.properties.indexOf(p[j])>=0 && this.indexed.indexOf(p[j])<0) {
        this.indexed.push(p[j])
        ;('eq' in this.query[p[j]] ? this.equi : this.nonEqui).push(p[j])
      }
    }
  }

  this.extraneous = this.properties.filter(notContainedIn, this.indexed)
  this.permutations = permutations(this.equi, this.nonEqui)
  
  this.selectAccessPath()
  this.selectFilters()
}

// Select indexes for scanning
QueryPlan.prototype.selectAccessPath = function () {
  debug({query: this.query})
  debug({indexes: this.names})

  var candidates = this.selectCandidates(true, false)

  this.equiPaths = []
  this.accessPaths = this.selectWinners(candidates, this.equiPaths)

  debug({candidates: candidates})
  debug({winners: this.accessPaths})
}

// Select indexes for filtering
QueryPlan.prototype.selectFilters = function () {
  var filterCandidates = this.selectCandidates(false, true)
  this.filters = this.selectWinners(filterCandidates)
}

// Select indexes until all predicates are represented
// TODO: for scanning, we'll likely use only one index.
// Once I have that part figured out, skip this and just
// use the first candidate.
QueryPlan.prototype.selectWinners = function (candidates, accEquality) {
  var represented = this.indexed.slice(), paths = []

  candidateLoop:
  for(var i=0, l=candidates.length; i<l && represented.length; i++) {
    var candidate = candidates[i]

    for(var j=0, jl=candidate.match.length; j<jl; j++) {
      var property = candidate.match[j]
      if (represented.indexOf(property) < 0) continue candidateLoop
    }

    represented = represented.filter(notContainedIn, candidate.match)

    var predicates = candidate.match.map(from, this.query)
      , a = exprToRange(predicates, true)

    var path = {
      index: candidate.index,
      map:   candidate.map,
      match: candidate.match,
      range: a[0]
    }

    if (accEquality != null && a[1] && candidate.fullRange)
      accEquality.push(path)

    paths.push(path)
  }

  return paths
}

/**
 * Select index candidates by finding an optimal index
 * for each possible set of properties. We can combine 
 * multiple eq operators and one (paired) lt/lte/gt/gte.
 * There's probably a lot of ways to optimize this part,
 * but first I gotta write more tests.
 */
QueryPlan.prototype.selectCandidates = function (contiguous, full) {
  var sets = this.permutations
    , candidates = Object.create(null)
    , optimalLength = full ? this.indexed.length : this.equi.length + (!!this.nonEqui.length)
    , represented = []

  for(var i=0, l=sets.length; i<l; i++) {
    var candidate = this.optimalIndex(sets[i], contiguous)
    if (!candidate) continue

    // In case candidate is selected more than once
    var current = candidates[candidate.name]
    if (current && optimalFirst(current, candidate) < 0) continue

    candidates[candidate.name] = candidate

    // Full match?
    if (candidate.match.length === optimalLength) break

    // Found index(es) for all props?
    candidate.match.forEach(pushUnique, represented)
    if (represented.length === optimalLength) break
  }

  // Convert to array
  var arr = Object.keys(candidates).map(from, candidates)

  // No indexes found
  if (!arr.length) return arr

  // Sort so that the first candidate has the most
  // matched and least unmatched properties
  arr.sort(optimalFirst)

  // We got a full match. Ignore the other indexes.
  if (arr[0].match.length === optimalLength) return [ arr[0] ]

  return arr
}

// Find the index with the most properties matching 
// a set (ordered list of properties). If contiguous
// is false, properties in the set may be skipped.
QueryPlan.prototype.optimalIndex = function (set, contiguous) {
  var best, max = 0, min = Infinity
    , setLength = set.length

  for(var i=0, l=this.names.length; i<l; i++) {
    var name  = this.names[i]
      , index = this.indexes[name]
      , props = index.properties
      , match = []

    for(var j=0; j<setLength; j++) {
      var offset = props.indexOf(set[j])
      if (contiguous && offset !== j) break
      if (offset >= 0) match.push(set[j])
    }

    var matched = match.length
      , unmatched = setLength - matched

    if (matched > max || (max > 0 && matched === max && unmatched < min)) {
      max = matched
      min = unmatched

      best = {
        index: index, name: name, map: index.map,
        match: match, unmatched: unmatched,
        fullRange: matched === props.length
      }
    }
  }

  return best
}

// Get possible combinations of properties
// [a,b,c] => [a,b,c], [a,b], [a], [c,b,a], etc
function permutations(equi, nonEqui) {
  var sets = equi.length ? permutationCombination(equi).toArray() : []
    , non = nonEqui.length

  // non-equi properties should always be last
  for(var i=0, l=sets.length || 1; i<l && non; i++) {
    var set = sets[i] || []
    for(var j=0; j<non; j++) {
      sets.push(set.concat(nonEqui[j]))
      if (equi.length) sets.push([ nonEqui[j] ])
    }
  }

  sets.sort(longestFirst)
  return sets
}

function longestFirst(a, b){
  return b.length - a.length
}

function optimalFirst(a, b){
  var matched = b.match.length - a.match.length
  if (matched!==0) return matched
  return a.unmatched - b.unmatched
}

function from(item){
  return this[item]
}

function pushUnique(item) {
  if (this.indexOf(item)<0) this.push(item)
}

function containedIn(item){ 
  return this.indexOf(item) >= 0
}

function notContainedIn(item) {
  return this.indexOf(item) < 0
}
