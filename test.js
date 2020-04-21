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
})
