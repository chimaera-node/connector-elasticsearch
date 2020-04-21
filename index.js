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

// config {...} => client {...} => query {
//   query: $ => ({
//     filter: $.and(
//       $.eq('a', 'hey'), // term
//       $.like('b', 'yo'), // match (fuzzy)
//       $.gte('c', 1), $.lte('c', 6), // range
//       $.exists('d'), // exists
//       $.or(
//         $.eq('e', 'hey', 'yo'), // terms
//         $.eq('path.to.f', 'hey', 'yo'), // nested terms
//         $.geo('g', { lat: 123, lon: 21, distance: '50mi' }), // geo distance
//       ),
//     ),
//     sort: $.orderBy($.asc('c'), $.desc('d')),
//     sort: $.functionScore(
//       $.gauss('l', { origin: '1998-01-01', scale: '1825d' }),
//       $.gauss('m', { origin: { lat: 34, lon: -118 }, scale: '100mi' }),
//       $.fieldValueFactor('n', { factor: 5, missing: 0, modifier: 'ln2p' }),
//     ),
//     limit: 100,
//     cursor: any|null,
//   }),
// } => query_result {
//   result: [{ _id: '1', _source: {...}, [_score: 5] }],
//   cursor: any|null,
//   meta: {
//     count: 100,
//   },
// }
const query = config => client => _.flow(
  _.get('query'),
  queryLexer,
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

module.exports = connectorElasticsearch
