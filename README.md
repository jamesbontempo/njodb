# njodb

`njodb` is a partitioned, multi-user-safe, Node.js JSON object datastore.

```js
const njodb = require("njodb");
const db = new njodb.Database();

const data = [
    {
        id: 1,
        name: "James",
        nickname: "Good Times",
        modified: Date.now()
    },
    {
        id: 2,
        name: "Steve",
        nickname: "Esteban",
        modified: Date.now()
    }
];

db.insert(data);

db.select(function(record) { return record.id === 1 || record.name === "Steve"});

db.update(function(record) { return record.name === "James"}, function(record) { record.nickname = "Bulldog" });

db.delete(function(record) { return record.modified < Date.now()});