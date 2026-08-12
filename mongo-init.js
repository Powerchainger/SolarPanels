// Creates the least-privilege application user for the solar_scraper database.
//
// Runs ONLY on a fresh/empty mongo-data volume (the official mongo image
// executes /docker-entrypoint-initdb.d/*.js during first-time init, when the
// temporary server has no auth). On an existing volume this script is never
// run — migrate those with the two manual createUser commands documented in
// the README instead.
//
// The MONGO_INITDB_ROOT_USERNAME/PASSWORD env vars only create the root/admin
// user, so the app user must come from here. Credentials are read from the
// mongo service environment (interpolated from .env by compose).
const DB_NAME = "solar_scraper";

const user = process.env.MONGO_APP_USER;
const pass = process.env.MONGO_APP_PASSWORD;

if (!user || !pass) {
  // Fail the container start: a healthy Mongo that refuses the app's
  // credentials is a worse failure mode than a container that never comes up.
  print(
    "ERROR: MONGO_APP_USER / MONGO_APP_PASSWORD not set — " +
      "cannot create the application user. Add them to .env and retry.",
  );
  quit(1);
}

const target = db.getSiblingDB(DB_NAME);

if (target.getUser(user)) {
  print(`app user '${user}' already exists on ${DB_NAME} — nothing to do`);
} else {
  target.createUser({
    user: user,
    pwd: pass,
    roles: [{ role: "readWrite", db: DB_NAME }],
  });
  print(`created app user '${user}' on ${DB_NAME} (readWrite)`);
}
