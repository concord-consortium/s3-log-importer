const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const format = require('pg-format');
const Cursor = require("pg-cursor")

const BATCH_SIZE = 100
const dateRegEx = /^\d\d\d\d-\d\d-\d\d$/

const [_node, _script, ...rest] = process.argv
if (rest.length !== 2) {
  die("Usage: npm run pull-logs <START-DATE> <END-DATE>")
}
let [startDate, endDate] = rest
if (!dateRegEx.test(startDate) || !dateRegEx.test(endDate)) {
  die("Start and end dates must be in the form YYYY-MM-DD")
}
startDate = `${startDate}T00:00:00.000Z`
endDate = `${endDate}T23:59:59.999Z`

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    const query = {
      text: "SELECT id, time, DATE(time) AS time_date FROM logs_meta WHERE time >= $1 AND time <= $2 ORDER BY DATE(time)",
      values: [startDate, endDate]
    }
    const cursor = localClient.query(new Cursor(query.text, query.values))

    let currentDate
    let currentDateRows = []

    const emitParquetFile = () => {
      return new Promise((resolve) => {
        if (currentDate && (currentDateRows.length > 0)) {
          const ids = []
          const timeMap = {}
          currentDateRows.forEach(row => {
            ids.push(row.id)
            timeMap[row.id] = row.time
          })

          remoteClient.query(format("SELECT id, session, username, application, activity, event, parameters, extras, event_value, run_remote_endpoint FROM logs WHERE id IN (%L) ORDER BY id", ids))
            .then(result => {
              console.log(`${batchNum++}: Found ${rows.length} rows in batch on remote database, inserting into local database`)

              // TODO:
              // 1. normalize the extras and parameters
              // 2. create parquet file with date path
              // 3. save parquet file on S3
              resolve()
            })
        } else {
          resolve()
        }
      })
    }

    let batchNum = 1
    const processResults = () => {
      return new Promise((resolve, reject) => {
        (function read() {
          cursor.read(BATCH_SIZE, async (err, rows) => {
            if (err) {
              return emitParquetFile().then(() => reject(err))
            }

            if (!rows.length) {
              return emitParquetFile().then(() => resolve())
            }

            const promises = rows.map(row => {
              if (row.time_date != currentDate) {
                return emitParquetFile().then(() => {
                  currentDate = row.time_date
                  currentDateRows = [row]
                })
              } else {
                return new Promise((resolve) => {
                  currentDateRows.push(row)
                  resolve()
                })
              }
            })

            return Promise.all(promises).then(() => read())
          });
        })();
      });
    }

    return processResults()
  })
  .catch(err => console.error(err))
  .finally(() => {
    console.log("Parquet files uploaded to S3.")
    process.exit()
  })
})