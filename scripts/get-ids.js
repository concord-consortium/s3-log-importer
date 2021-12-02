const {localConnect, remoteConnect} = require("./shared/db")

localConnect().then(localClient => {
  remoteConnect().then(remoteClient => {
    return localClient.query("SELECT MIN(remote_id) AS min_remote_id, MAX(remote_id) AS max_remote_id FROM logs_meta")
      .then(result => console.log("LOCAL", result.rows))
      .then(() => {
        return remoteClient.query("SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM logs")
          .then(result => console.log("REMOTE", result.rows))
        })
  })
  .then(() => process.exit())
})