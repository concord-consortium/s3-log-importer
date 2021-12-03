const {localConnect} = require("./shared/db")
const format = require('pg-format');

const WINDOW_SIZE = 100

/*
1. find all times outside of range
2. for each time find the nearest time in range
3. average the two times
*/

localConnect().then(localClient => {
  const getOutOfRangeRows = () => {
    const query = `
      SELECT remote_id
      FROM logs_meta
      WHERE time < '2014-01-01T00:00:00.000Z' OR time > now()::timestamp
    `
    return localClient.query(query)
      .then(result => {
        const fixedTimeMap = {}
        console.log(`Finding nearest time for ${result.rows.length} rows with bad times`)
        const promises = result.rows.map(row => {
          const closestIdsQuery = format(`
            SELECT remote_id, extract(epoch from time) as unix_time, time FROM
            (
              (SELECT * FROM logs_meta WHERE remote_id >= %L AND time >= '2014-01-01T00:00:00.000Z' AND time <= now()::timestamp ORDER BY remote_id LIMIT 1)
              UNION ALL
              (SELECT * FROM logs_meta WHERE remote_id < %L AND time >= '2014-01-01T00:00:00.000Z' AND time <= now()::timestamp ORDER BY remote_id DESC LIMIT 1)
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
          time = to_timestamp(%L) at time zone 'utc',
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