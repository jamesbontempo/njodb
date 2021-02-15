# njodb

`njodb` is a partitioned, multi-user-safe, Node.js JSON object datastore.

Load the module:
```js
const njodb = require("njodb");
```

Create an instance of an NJODB:
```js
const db = new njodb.Database();
```

Create some JSON data objects:
```js
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
```

Insert them into the database:
```js
db.insert(data);
```

Select some records from the database by supplying a function find matches:
```js
db.select(function(record) { return record.id === 1 || record.name === "Steve"});
```

Update some records in the database by supplying a function find matches and another function to update them:
```js
db.update(function(record) { return record.name === "James"}, function(record) { record.nickname = "Bulldog" });
```

Delete some records from the database by supplying a function find matches:
```js
db.delete(function(record) { return record.modified < Date.now()});
```