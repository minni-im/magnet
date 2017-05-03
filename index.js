require("colors");
const argv = require("minimist")(process.argv.slice(2));
const moment = require("moment");
const fs = require("fs");
const path = require("path");
const nano = require("nano")("http://localhost:5984");

if (argv._.length === 0) {
  console.log("Script Error:".bold.red, "You have to specify a path to a folder that contains migration files", "\n");
  console.log("Usage: npm run-script migrate migration-folder\n");
  process.exit(1);
}

const migrationFolder = argv._[0];
const viewName = "all";
let designDoc;
let db;
let batchSize = argv.s || 1000;
let dbName = argv.db;
let migrationFiles;

function log(a, b, c, d, e, f) {
  var text = [].concat([" ├─ ", a, b, c, d, e, f]).join("");
  process.stdout.write(text.grey + "\n");
}

function getItemsFromView(dDoc, vName, params, progress) {

  return new Promise(function(resolve, reject) {
    if (progress >= batchSize) {
      process.stdout.write((" ├─ " + progress + "…" + (progress + batchSize) + " ").grey);
    }
    db.view(dDoc, vName, params, function(error, body) {
      if (error) {
        return reject(error);
      }
      if (progress === 0) {
        batchSize = Math.min(body.total_rows, batchSize);  
        console.log((" ├─ " + body.total_rows + " documents to be migrated").grey);
        if (body.total_rows > batchSize) {
          console.log((" ├─ Batching by group of " + batchSize + " item(s)").grey);
        }
        process.stdout.write((" ├─ " + progress + "…" + (progress + batchSize) + " ").grey);
      }
      return resolve(body.rows);
    });
  });
}

function processBatch(processor) {
  return function(documents) {
    return Promise.all(documents.map(function(document) {
      var result;
      try {
        result = processor.process(document.value, log);
      } catch(err) {
        process.stdout.write("─".grey + " PROCESSING ".bold);
        console.log("", " FAIL ".bgRed.bold.white);
        console.log(" ERROR ".bgRed.bold.white, "Processing document with id", "'" + document.value._id + "'", " failed");
        console.log(err);
        process.exit(1);
      }
      return result;
    }));
  };
}

function saveBatch(documents) {
  return new Promise(function(resolve, reject) {
    var bulkData = {
      "docs": documents
    };
    process.stdout.write("─".grey + " BULK UPDATE ".bold);
    db.bulk(bulkData, function(error) {
      if (error) {
        return reject(error);
      }
      console.log("", " OK ".bgGreen.bold);
      return resolve(documents);
    });
  });
}

function getViewInBatches(dDoc, vName, params, size, processor) {
  params.limit = size + 1;
  return new Promise(function(resolve, reject) {
    var count = 0;
    (function nextBatch() {
      var next;
      getItemsFromView(dDoc, vName, params, count)
        .then(function(documents) {
          next = documents[size];
          documents = documents.slice(0, Math.max(1, size));
          if (next) {
            params.startkey = next.key;
            if (next.id) {
              params.startkey_docid = next.id;
            }
          }
          count += documents.length;
          return documents;
        })
        .then(processBatch(processor))
        .then(saveBatch)
        .then(function() {
          if (next) {
            return nextBatch();
          } else {
            return resolve(count);
          }
        }).catch(function(err) {
          return reject(err);
        });
    })();
  });
}

console.log("COUCHDB MIGRATION TOOL".bold.white);

try {
  migrationFiles = fs.readdirSync(path.resolve(migrationFolder));
} catch(e) {
  console.log("Script Error:".bold.red, "'" + migrationFolder + "'", "does not exist!\n");
  process.exit(1);
}

console.log("" + migrationFiles.length + " file(s) detected in", migrationFolder.bold, "->", migrationFiles.join(", "));

function processMigration(file) {
  var migration = require(path.resolve(migrationFolder, file));
  designDoc = path.basename(file, ".js");
  batchSize = migration.batchSize || batchSize;

  if(!migration.db && !dbName) {
    console.log("Script Error:".bold.red, "you must specify a database name to be used. Simply export a 'db' property from your migration file, or using cli " + "--db".grey +" parameter.\n");
    process.exit(1);
  }
  db = nano.use(migration.db || dbName);

  var start = Date.now();
  console.log("");
  console.log((" " + designDoc + "/" + viewName + " ").bgCyan.grey + " MIGRATION START ".cyan);

  return getViewInBatches(designDoc, viewName, {}, batchSize, migration)
    .then(function(total) {
      console.log((" " + designDoc + "/" + viewName + " ").bgCyan.grey + " MIGRATION DONE ".bold.cyan);
      var end = Date.now();
      console.log(("Total of " + total + " document(s) processed").green, "in", moment.duration(end - start).asSeconds() + "s");

    }).catch(function(err) {
      console.log();
      console.log(" ERROR ".bgRed.bold.white, err);
      process.exit(1);
    });
}

migrationFiles.reduce(
  (pChain, file) => pChain.then(
    () => processMigration(file),
    (error) => console.log(" ERROR ".bgRed.bold.white, error)
  ), Promise.resolve(true));
