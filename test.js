const _ = require('rubico')
const a = require('a-sert')
const exception = require('@chimaera/exception')
const connectorElasticsearch = require('.')

describe('connectorElasticsearch', () => {
  it('establishes an elasticsearch connection', () => _.flow(
    connectorElasticsearch,
    a.eq('chimaera://[elasticsearch::local]/my_index', _.get('uri')),
    a.eq('my_index', _.get('prefix')),
    a.eq(1, _.get('client.connectionPool.size')),
    a.eq('localhost', _.get('client.connectionPool.connections.0.url.hostname')),
    a.eq('http:', _.get('client.connectionPool.connections.0.url.protocol')),
    a.eq('9200', _.get('client.connectionPool.connections.0.url.port')),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({
      _id: '1',
      _source: {
        name: 'george j',
        birthday: '1998-03-01',
        location: { lat: 34, lon: -118 },
      },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: {
        name: 'george j',
        birthday: '1998-03-01',
        location: { lat: 34, lon: -118 },
      },
    })('1')),
    _.effect(conn => conn.update({
      _id: '1',
      _source: { name: 'george james' },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: {
        name: 'george james',
        birthday: '1998-03-01',
        location: { lat: 34, lon: -118 },
      },
    })('1')),
    _.effect(conn => conn.put({ _id: '1', _source: {} })),
    _.effect(conn => a.eq(conn.get, { _id: '1', _source: {} })('1')),
    _.effect(conn => conn.delete('1')),
    _.effect(conn => a.err(conn.get, exception.ResourceNotFound(`${conn.uri}/1`))('1')),
    _.effect(conn => conn.free()),
  )({
    connector: 'elasticsearch',
    env: 'local',
    prefix: 'my_index',
    endpoint: 'http://localhost:9200',
  }))

  it('queries', () => _.flow(
  )())

/* {
  filter: {
    operator: 'and',
    operations: [
      { operator: 'eq', field: 'a', values: ['hey'] },
      { operator: 'neq', field: 'a', values: ['heya'] },
      { operator: 'lt', field: 'b', values: [6] },
      { operator: 'gt', field: 'c', values: [0] },
      { operator: 'lte', field: 'd', values: [5] },
      { operator: 'gte', field: 'e', values: [1] },
      { operator: 'prefix', field: 'f', values: ['wazz'] },
      {
        operator: 'or',
        operations: [
          { operator: 'eq', field: 'h', values: ['yo'] },
          { operator: 'exists', field: 'i', values: [] },
          { operator: 'dne', field: 'j', values: [] },
        ],
      },
    ],
  },
  sort: {
    operator: 'orderBy',
    operations: [
      { operator: 'asc', field: 'b', values: [] },
      { operator: 'desc', field: 'c', values: [] },
    ],
  },
  limit: 100,
} */

  describe('toQueryDSL', () => {
    it('transforms a lexed query object to elasticsearch query dsl - filter', () => _.flow(
      connectorElasticsearch.toQueryDSL,
      a.eq(_.id, {
        bool: {
          filter: [
            { term: { a: { value: 'hey' } } },
            { bool: { must_not: [{ term: { a: { value: 'heya' } } }] } },
            { range: { b: { lt: 6 } } },
            { range: { c: { gt: 0 } } },
            { range: { d: { lte: 5 } } },
            { range: { e: { gte: 1 } } },
            { prefix: { f: { value: 'wazz' } } },
            { dis_max: { queries: [
              { terms: { h: ['yo', 'yoyo'] } },
              { exists: { field: 'i' } },
              { bool: { must_not: [{ exists: { field: 'j' } }] } },
            ] } },
          ],
        },
      }),
    )({
      operator: 'and',
      operations: [
        { operator: 'eq', field: 'a', values: ['hey'] },
        { operator: 'neq', field: 'a', values: ['heya'] },
        { operator: 'lt', field: 'b', values: [6] },
        { operator: 'gt', field: 'c', values: [0] },
        { operator: 'lte', field: 'd', values: [5] },
        { operator: 'gte', field: 'e', values: [1] },
        { operator: 'prefix', field: 'f', values: ['wazz'] },
        {
          operator: 'or',
          operations: [
            { operator: 'eq', field: 'h', values: ['yo', 'yoyo'] },
            { operator: 'exists', field: 'i', values: [] },
            { operator: 'dne', field: 'j', values: [] },
          ],
        },
      ],
    }))
  })
})
