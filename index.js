const _ = require('rubico')
const exception = require('@chimaera/exception')
const queryLexer = require('@chimaera/query-lexer')
const uri = require('@chimaera/uri')
const elasticsearch = require('@elastic/elasticsearch')

const getIndexName = config => `${config.env}-${config.prefix}`

// config => client => () => undefined
const init = config => client => _.flow(
  _.diverge({
    index: getIndexName(config),
    body: {
      mappings: {
        dynamic_date_formats: ['yyyy-MM-dd'],
        properties: {
          location: { type: 'geo_point' },
          create_time: { type: 'date', format: 'epoch_millis' },
          update_time: { type: 'date', format: 'epoch_millis' },
        },
      },
    },
  }),
  _.tryCatch(_.flow(
    client.indices.create,
    _.get('body.acknowledged'),
  ), _.switch(
    _.eq('resource_already_exists_exception', _.get('message')),
    _.noop,
    _.throw,
  )),
)

// config => client => () => undefined
const free = config => client => _.flow(
  _.diverge({ index: getIndexName(config) }),
  _.tryCatch(
    client.indices.delete,
    _.switch(
      _.eq('index_not_found_exception', _.get('message')),
      _.id,
      _.throw,
    ),
  ),
  _.noop,
)

// config => client => () => undefined
const ready = config => client => _.flow(
  _.diverge({ index: getIndexName(config) }),
  client.indices.exists,
  _.get('body'),
  _.switch(_.id, _.noop, _.flow(
    () => uri.encodePublic(config),
    exception.ConnectionNotReady,
    _.throw,
  )),
)

// config => client => resource => status
const put = config => client => _.flow(
  _.diverge({
    index: getIndexName(config),
    id: _.get('_id'),
    body: _.get('_source'),
    refresh: _.flow(_.get('$refresh', false), _.toString),
  }),
  client.index,
  _.noop,
)

// config => client => id => resource
const get = config => client => _.flow(
  _.diverge({ index: getIndexName(config), id: _.id }),
  _.tryCatch(
    _.flow(
      client.get,
      _.diverge({
        _id: _.get('body._id'),
        _source: _.get('body._source'),
      }),
    ),
    _.switch(
      _.eq(404, _.get('meta.statusCode')),
      _.flow(
        _.diverge([uri.encodePublic(config), _.get('meta.body._id')]),
        _.join('/'),
        exception.ResourceNotFound,
        _.throw,
      ),
      _.throw,
    ),
  ),
)

// config => client => resource => status
const update = config => client => _.flow(
  _.diverge({
    index: getIndexName(config),
    id: _.get('_id'),
    body: _.diverge({ doc: _.get('_source') }),
    refresh: _.flow(_.get('$refresh', false), _.toString),
    _source: false,
  }),
  client.update,
  () => ({ ok: true }),
)

// config => client => id => status
const del = config => client => _.flow(
  _.diverge({ index: getIndexName(config), id: _.id }),
  client.delete,
  _.noop,
)

const toQueryDSL = x => {
  if (x.operator === 'and') return {
    bool: { filter: _.map.sync(toQueryDSL)(x.operations) },
  }
  if (x.operator === 'or') return {
    dis_max: { queries: _.map.sync(toQueryDSL)(x.operations) },
  }
  if (x.operator === 'eq') {
    if (x.values.length < 1) {
      throw exception.MalformedQuery('eq value[s] required')
    }
    if (x.values.length === 1) return {
      term: { [x.field]: { value: x.values[0] } },
    }
    return { terms: { [x.field]: x.values } }
  }
  if (x.operator === 'neq') {
    if (x.values.length < 1) {
      throw exception.MalformedQuery('neq value[s] required')
    }
    if (x.values.length === 1) return {
      bool: { must_not: [{ term: { [x.field]: { value: x.values[0] } } }] },
    }
    return { bool: { must_not: [{ terms: { [x.field]: x.values } }] } }
  }
  if (x.operator === 'lt') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('lt arguments')
    }
    return { range: { [x.field]: { lt: x.values[0] } } }
  }
  if (x.operator === 'gt') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('gt arguments')
    }
    return { range: { [x.field]: { gt: x.values[0] } } }
  }
  if (x.operator === 'lte') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('lte arguments')
    }
    return { range: { [x.field]: { lte: x.values[0] } } }
  }
  if (x.operator === 'gte') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('gte arguments')
    }
    return { range: { [x.field]: { gte: x.values[0] } } }
  }
  if (x.operator === 'exists') return {
    exists: { field: x.field },
  }
  if (x.operator === 'dne') return {
    bool: { must_not: [{ exists: { field: x.field } }] },
  }
  if (x.operator === 'prefix') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('prefix arguments')
    }
    return { prefix: { [x.field]: { value: x.values[0] } } }
  }
  if (x.operator === 'like') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('like arguments')
    }
    return { match: { [x.field]: { query: x.values[0] } } }
  }
  if (x.operator === 'geo') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('geo arguments')
    }
    return {
      geo_distance: {
        distance: x.distance,
        [x.field]: { lat: x.lat, lon: x.lon },
      },
    }
  }
  if (x.operator === 'asc') {
    return { [x.field]: { order: 'asc' } }
  }
  if (x.operator === 'desc') {
    return { [x.field]: { order: 'desc' } }
  }
  if (x.operator === 'gauss') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('gauss arguments')
    }
    return { gauss: { [x.field]: x.values[0] } }
  }
  if (x.operator === 'fieldValueFactor') {
    if (x.values.length !== 1) {
      throw exception.MalformedQuery('fieldValueFactor arguments')
    }
    return { fieldValueFactor: { field: x.field, ...x.values[0] } }
  }
  throw exception.UnsupportedSyntax(x.operator)
}

/*
config {...} => client {...} => query {
  filter: $ => $.and(
    $.eq('a', 'hey'), // term
    $.like('b', 'yo'), // match (fuzzy)
    $.gte('c', 1), $.lte('c', 6), // range
    $.exists('d'), // exists
    $.or(
      $.eq('e', 'hey', 'yo'), // terms
      $.eq('path.to.f', 'hey', 'yo'), // nested terms
      $.geo('g', { lat: 123, lon: 21, distance: '50mi' }), // geo distance
    ),
  ),
  sort: $ => $.orderBy($.asc('c'), $.desc('d')),
  sort: $ => $.functionScore(
    $.gauss('l', { origin: '1998-01-01', scale: '1825d' }),
    $.gauss('m', { origin: { lat: 34, lon: -118 }, scale: '100mi' }),
    $.fieldValueFactor('n', { factor: 5, missing: 0, modifier: 'ln2p' }),
  ),
  limit: 100,
  cursor: any|null,
} => query_result {
  result: [{ _id: '1', _source: {...}, [_score: 5] }],
  cursor: any|null,
  meta: {
    count: 100,
  },
}
*/
const query = config => client => _.flow(
  _.map(_.flow(_.toFn, queryLexer)),
  _.switch(
    _.not(_.eq(_.get('filter.operator'), 'and')),
    _.flow(_.get('filter.operator'), exception.UnsupportedSyntax, _.throw),
    _.id,
  ),
  _.switch(
    _.eq(_.get('sort.operator'), 'orderBy'),
    _.diverge({
      query: _.flow(_.get('filter'), toQueryDSL),
      sort: _.flow(_.get('sort.operations'), _.map(toQueryDSL)),
      size: _.get('limit'),
      from: _.get('cursor'),
    }),
    _.eq(_.get('sort.operator'), 'functionScore'),
    _.diverge({
      query: _.diverge({
        function_score: _.diverge({
          query: _.flow(_.get('filter'), toQueryDSL),
          functions: _.flow(_.get('sort.operations'), _.map(toQueryDSL)),
          score_mode: _.get('score_mode', 'multiply'),
        }),
      }),
      size: _.get('limit'),
      from: _.get('cursor'),
    }),
    _.flow(_.get('sort.operator'), exception.UnsupportedSyntax, _.throw),
  ),
  client.query,
)

/*
 * config => connection {
 *   uri: string,
 *   prefix: string,
 *   client: object,
 *   ready: () => undefined,
 *   put: object => undefined,
 *   get: string => object,
 *   update: object => object,
 *   delete: string => undefined,
 *   init: () => undefined,
 *   free: () => undefined,
 * }
 */
const connectorElasticsearch = _.flow(
  _.diverge([
    _.id,
    _.flow(
      _.diverge({
        node: _.get('endpoint'),
        maxRetries: 5,
      }),
      x => new elasticsearch.Client(x),
    ),
  ]),
  _.diverge({
    uri: _.flow(_.get(0), uri.encodePublic),
    prefix: _.get([0, 'prefix']),
    client: _.get(1),
    ready: _.apply(ready),
    put: _.apply(put),
    get: _.apply(get),
    update: _.apply(update),
    delete: _.apply(del),
    init: _.apply(init),
    free: _.apply(free),
  }),
)

connectorElasticsearch.toQueryDSL = toQueryDSL

module.exports = connectorElasticsearch
