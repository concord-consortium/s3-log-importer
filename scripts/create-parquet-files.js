const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const {parse} = require("@concord-consortium/hstore-to-json")()
const parquet = require('parquets');
const mkdirp = require('mkdirp')
const format = require('pg-format');
const Cursor = require("pg-cursor")

const BATCH_SIZE = 1000

const dateRegEx = /^\d\d\d\d-\d\d-\d\d$/

const [_node, _script, ...rest] = process.argv
if (rest.length == 0) {
  die("Usage: npm run pull-logs <START-DATE> [<END-DATE>]")
}
let [startDate, endDate] = rest
endDate = endDate || startDate
if (!dateRegEx.test(startDate) || !dateRegEx.test(endDate)) {
  die("Start and end dates must be in the form YYYY-MM-DD")
}

const startOfDay = (ymd) => `${ymd}T00:00:00.000Z`
const endOfDay = (ymd) => `${ymd}T23:59:59.999Z`
const zeroPad = (n) => n < 10 ? `0${n}` : `${n}`
const dateString = (date) => `${date.getUTCFullYear()}-${zeroPad(date.getUTCMonth() + 1)}-${zeroPad(date.getUTCDate())}`
const s3Path = (date) => `${date.getUTCFullYear()}/${zeroPad(date.getUTCMonth() + 1)}/${zeroPad(date.getUTCDate())}/00`
const toJsonString = (o) => JSON.stringify(parse(o))

const schema = new parquet.ParquetSchema({
  session: {type: "UTF8"},
  username: {type: "UTF8"},
  application: {type: "UTF8"},
  activity: {type: "UTF8"},
  event: {type: "UTF8"},
  event_value: {type: "UTF8"},
  time: {type: "INT_64"},
  parameters: {type: "UTF8"},
  extras: {type: "UTF8"},
  run_remote_endpoint: {type: "UTF8"},
  timestamp: {type: "INT_64"},
});

startDate = startOfDay(startDate)
endDate = endOfDay(endDate)

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    const query = {
      text: "SELECT DATE(timestamp) AS time_date FROM logs_meta WHERE timestamp >= $1 AND timestamp <= $2 GROUP BY DATE(timestamp) ORDER BY DATE(timestamp) ASC",
      values: [startDate, endDate]
    }
    return localClient.query(query)
      .then(result => {
        const promises = result.rows.map(row => {
          const timeDate = row.time_date
          const ymd = dateString(timeDate)
          const query = {
            text: "SELECT remote_id, extract(epoch from timestamp) as timestamp FROM logs_meta WHERE time >= $1 AND time <= $2",
            values: [startOfDay(ymd), endOfDay(ymd)]
          }
          return localClient.query(query)
            .then(result => {
              const timestampMap = {}
              result.rows.forEach(row => {
                timestampMap[row.remote_id] = row.timestamp
              })
              const ids = result.rows.map(row => row.remote_id)

              const query = format("SELECT id, session, username, application, activity, event, event_value, extract(epoch from time) as time, parameters, extras, run_remote_endpoint FROM logs WHERE id IN (%L) ORDER BY id", ids)
              const cursor = remoteClient.query(new Cursor(query))

              let batchNum = 1
              const createParquetFile = async () => {
                const subPath = `./output/${s3Path(timeDate)}`
                mkdirp.sync(subPath)
                const outputPath = `${subPath}/${ymd}.parquet`
                console.log(`Creating ${outputPath} with ${ids.length} rows`)
                const writer = await parquet.ParquetWriter.openFile(schema, outputPath)

                return new Promise((resolve, reject) => {
                  (function read() {
                    cursor.read(BATCH_SIZE, async (err, rows) => {
                      if (err) {
                        await writer.close()
                        return reject(err);
                      }

                      if (!rows.length) {
                        await writer.close()
                        return resolve();
                      }

                      console.log(`${ymd} ${batchNum++}: inserting ${rows.length} rows into parquet file`)

                      rows = rows.map(row => {
                        const {id, session, username, application, activity, event, event_value, time, parameters, extras, run_remote_endpoint, timestamp} = row
                        return {
                          session: session || "",
                          username: username || "",
                          application: application || "",
                          activity: activity || "",
                          event: event || "",
                          event_value: event_value || "",
                          time: time || 0,
                          parameters: toJsonString(row.parameters),
                          extras: toJsonString(row.extras),
                          run_remote_endpoint: run_remote_endpoint || "",
                          timestamp: timestampMap[id] || 0,
                        }
                      })
                      for (row of rows) {
                        await writer.appendRow(row)
                      }
                      return read()
                    });
                  })();
                });
              }

              return createParquetFile()
            })
        })
        return Promise.all(promises)
      })
  })
  .catch(err => console.error(err))
  .finally(() => {
    console.log("Parquet files created.")
    process.exit()
  })
})