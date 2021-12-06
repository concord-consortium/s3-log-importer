const {localConnect, remoteConnect} = require("./shared/db")

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    return localClient.query("SELECT MIN(time) AS min_time, MAX(time) AS max_time, MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp FROM logs_meta")
      .then(result => console.log("LOCAL", result.rows))
      .then(() => {
        return remoteClient.query("SELECT MIN(time) AS min_time, MAX(time) AS max_time FROM logs")
          .then(result => console.log("REMOTE", result.rows))
        })
  })
  .then(() => process.exit())
})