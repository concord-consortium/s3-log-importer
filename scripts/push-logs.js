const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const {parse} = require("@concord-consortium/hstore-to-json")()
const parquet = require('parquets');

const BATCH_SIZE = 100
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
const toJsonString = (o) => JSON.stringify(parse(o))

const schema = new parquet.ParquetSchema({
  session: {type: "UTF8"},
  username: {type: "UTF8"},
  application: {type: "UTF8"},
  activity: {type: "UTF8"},
  event: {type: "UTF8"},
  event_value: {type: "UTF8"},
  time: {type: "INT_32"},
  parameters: {type: "UTF8"},
  extras: {type: "UTF8"},
  run_remote_endpoint: {type: "UTF8"}
});

startDate = startOfDay(startDate)
endDate = endOfDay(endDate)

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    const query = {
      text: "SELECT COUNT(remote_id) as num_logs, DATE(time) AS time_date FROM logs_meta WHERE time >= $1 AND time <= $2 GROUP BY DATE(time) ORDER BY DATE(time) ASC",
      values: [startDate, endDate]
    }
    return localClient.query(query)
      .then(result => {
        const promises = result.rows.map(row => {
          const ymd = dateString(row.time_date)
          const query2 = {
            text: "SELECT session, username, application, activity, event, event_value, extract(epoch from time) as time, parameters, extras, run_remote_endpoint FROM logs WHERE time >= $1 AND time <= $2 ORDER BY id",
            values: [startOfDay(ymd), endOfDay(ymd)]
          }
          return remoteClient.query(query2)
            .then(async result2 => {
              const rows = result2.rows.map(row => {
                const {session, username, application, activity, event, event_value, time, parameters, extras, run_remote_endpoint, timestamp} = row
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
                  run_remote_endpoint: run_remote_endpoint || ""
                }
              })

              const outputPath = `./output/${ymd}.parquet`
              if (rows.length > 0) {
                // create parquet file
                console.log(`Creating ${outputPath} with ${rows.length} rows`)
                const writer = await parquet.ParquetWriter.openFile(schema, outputPath)
                for (row of rows) {
                  await writer.appendRow(row)
                }
                return writer.close()
              } else {
                console.log(`Skipping ${outputPath} - no data found`)
              }
            })
        })
        return Promise.all(promises)
      })
  })
  .catch(err => console.error(err))
  .finally(() => {
    console.log("Parquet files uploaded to S3.")
    process.exit()
  })
})