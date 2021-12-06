const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const format = require('pg-format');
const Cursor = require("pg-cursor")

const BATCH_SIZE = 1000

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
    const query = {
      text: "SELECT id, application, time FROM logs WHERE id >= $1 AND id <= $2",
      values: [startId, endId]
    }
    const cursor = remoteClient.query(new Cursor(query.text, query.values))

    let batchNum = 1
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

            console.log(`${batchNum++}: Found ${rows.length} rows in batch on remote database, inserting into local database`)
            const values = rows.map(row => {
              const {id, application, time} = row
              return [id, application, time, time, 0]
            })
            return localClient.query(format("INSERT INTO logs_meta (remote_id, application, time, timestamp, status) VALUES %L ON CONFLICT (remote_id) DO NOTHING", values))
              .then(() => read())
          });
        })();
      });
    }

    return processResults()
  })
  .catch(err => console.error(err))
  .finally(() => {
    console.log("Rows inserted to local database (conflicts skipped).")
    process.exit()
  })
})