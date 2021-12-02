const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const format = require('pg-format');

const [_node, _script, ...rest] = process.argv
if (rest.length !== 2) {
  die("Usage: npm run pull-logs <START-ID> <END-ID>")
}
let [startId, endId] = rest
startId = parseInt(startId, 10)
endId = parseInt(endId, 10)
if (isNaN(startId) || isNaN(endId)) {
  die("Start and end ids must be integers")
}

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    return remoteClient.query("SELECT id, application, time FROM logs WHERE id >= $1 AND id <= $2", [startId, endId])
      .then(result => {
        console.log(`Found ${result.rowCount} rows on remote database, inserting into local database`)
        const values = result.rows.map(row => {
          const {id, application, time} = row
          return [id, application, time, 0]
        })
        return localClient.query(format("INSERT INTO logs_meta (remote_id, application, time, status) VALUES %L ON CONFLICT (remote_id) DO NOTHING", values))
      })
      .then(() => console.log("Rows inserted to local database (conflicts skipped)."))
  })
  .then(() => process.exit())
})