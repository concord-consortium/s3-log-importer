const {localConnect} = require("./shared/db")
const format = require('pg-format');

localConnect().then(localClient => {
  const getOutOfRangeRows = () => {
    const query = `
      SELECT remote_id
      FROM logs_meta
      WHERE timestamp < '2014-01-01T00:00:00.000Z' OR timestamp > now()::timestamp
    `
    return localClient.query(query)
      .then(result => {
        const fixedTimeMap = {}
        console.log(`Finding nearest timestamp for ${result.rows.length} rows with bad times`)
        const promises = result.rows.map(row => {
          const closestIdsQuery = format(`
            SELECT remote_id, extract(epoch from timestamp) as unix_time FROM
            (
              (SELECT * FROM logs_meta WHERE remote_id >= %L AND timestamp >= '2014-01-01T00:00:00.000Z' AND timestamp <= now()::timestamp ORDER BY remote_id LIMIT 1)
              UNION ALL
              (SELECT * FROM logs_meta WHERE remote_id < %L AND timestamp >= '2014-01-01T00:00:00.000Z' AND timestamp <= now()::timestamp ORDER BY remote_id DESC LIMIT 1)
            ) AS foo
          `, row.remote_id, row.remote_id)

          return localClient.query(closestIdsQuery)
            .then(result => {
              if (result.rows.length !== 2) {
                console.error(`ONLY ${result.rows.length} ROW FOUND!`)
                process.exit()
              }
              fixedTimeMap[row.remote_id] = (result.rows[0].unix_time + result.rows[1].unix_time ) / 2
            })
        })

        return Promise.all(promises)
          .then(() => fixedTimeMap)
      })
  }

  const updateOutOfRangeRows = (fixedTimeMap) => {
    const ids = Object.keys(fixedTimeMap)
    console.log(`Found ${ids.length} rows to update`)
    const promises = ids.map(id => {
      const query = format(`
        UPDATE logs_meta
        SET
          timestamp = to_timestamp(%L) at time zone 'utc',
          status = status | 1
        WHERE remote_id = %L
      `, fixedTimeMap[id], id)
      return localClient.query(query)
    })
    return Promise.all(promises)
  }

  getOutOfRangeRows()
    .then(updateOutOfRangeRows)
    .catch(err => console.error(err))
    .finally(() => process.exit())
})