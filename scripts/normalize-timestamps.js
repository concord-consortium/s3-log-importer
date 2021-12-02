const {localConnect} = require("./shared/db")
const Cursor = require("pg-cursor")

const BATCH_SIZE = 100
const WINDOW_SIZE = 100

localConnect().then(localClient => {
  const query = `
    SELECT
      remote_id, time,
      AVG(extract(epoch from time))
    OVER (PARTITION BY remote_id ORDER BY remote_id ROWS BETWEEN ${WINDOW_SIZE} PRECEDING AND ${WINDOW_SIZE} FOLLOWING) AS time_average
    FROM logs_meta
    WHERE time < '2014-01-01T00:00:00.000Z' OR time > now()::timestamp
  `
  const cursor = localClient.query(new Cursor(query))

  const rowsToUpdate = []

  const processResults = () => {
    return new Promise((resolve, reject) => {
      (function read() {
        cursor.read(BATCH_SIZE, async (err, rows) => {
          if (err) {
            return reject(err);
          }

          if (!rows.length) {
            return resolve();
          }

          rowsToUpdate = rowsToUpdate.concat(rows)

          return read();
        });
      })();
    });
  }

  processResults()
    .then(() => {
      console.log("ROWS TO UPDATE", rowsToUpdate)
    })
    .catch(err => console.error(err))
    .finally(() => process.exit())
})