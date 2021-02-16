# njodb

`njodb` is a persistent, partitioned, concurrency-controlled, Node.js JSON object database. Data is written to the file system, distributed across multiple files that are protected by read and write locks.

## Table of contents
- [Install](#install)
- [Test](#test)
- [Introduction](#introduction)
- [Constructor](#constructor)
  - [Database properties](#database-properties)
- [Basic Methods](#basic-methods)
  - [getProperties](#getproperties)
  - [setProperties](#setproperties)
  - [getDebug](#getdebug)
  - [setDebug](#setdebug)
- [Data manipulation methods](#data-manipulation-methods)
  - [insert](#insert)
  - [select](#select)
  - [update](#update)
  - [delete](#delete)

## Install
```js
npm install njodb
```

## Test
```js
npm test
```

## Introduction
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

Select some records from the database by supplying a function to find matches:
```js
db.select(function(record) { return record.id === 1 || record.name === "Steve"});
```

Update some records in the database by supplying a function to find matches and another function to update them:
```js
db.update(function(record) { return record.name === "James"}, function(record) { record.nickname = "Bulldog"; return record; });
```

Delete some records from the database by supplying a function to find matches:
```js
db.delete(function(record) { return record.modified < Date.now()});
```

## Constructor
Creates a new instance of an NJODB `Database`.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`root`|string|path to the root directory of the `Database`|`.`

If the `root` directory does not exist it will be created, along with a default `njodb.properties` file and a default data directory (See [Database properties](#database-properties) below).

Example:

```js
const db = new njodb.Database() // created in the current directory

const db = new njodb.Databas("/path/to/some/other/place")
```

### Database properties
An NJODB `Database` has several properties that control its functioning. These properties can be set explicitly in the `njodb.properties` file in the `root` directory; otherwise, default properties will be used. For a newly created `Database`, an `njodb.properties` file will be created using default values.

Properties:

Name|Type|Description|Default
----|----|-----------|-------
`datadir`|string|The name of the subdirectory of `root` where data files will be stored|`data`
`dataname`|string|The file name that will be used when creating or accessing data files|`data`
`datastores`|number|The number of data stores, or data partitions, that will be used|`5`
`tempdir`|string|The name of the subdirectory of `root` where temporary data files will be stored|`tmp`
`lockoptions`|object|The options that will be used by [proper-lockfile](https://www.npmjs.com/package/proper-lockfile) to lock data files|`{"stale": 5000, "update": 1000, "retries": { "retries": 5000, "minTimeout": 250, "maxTimeout": 5000, "factor": 0.15, "randomize": false } }`
`debug`|boolean|Whether to print out debugging information to the console|`false`


## Basic Methods

### getProperties

Returns the properties set for the `Database`.

### setProperties

Sets the properties for the the `Database`.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`properties`|object|The properties to set for the `Database`|See [Database properties](#database-properties)

### getDebug

Returns the `debug` property for the `Database`.

### setDebug

Sets the `debug` property for the `Database`.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`debug`|boolean|The `debug` property to set for the `Database`|`false`

## Data manipulation methods

All data manipulation methods are asynchronous and return a `Promise`.

### insert

Inserts data into the `Database`.

Parameters:

Name|Type|Description
----|----|-----------
`data`|array|An array of JSON objects to insert into the `Database`

Resolves with an object containing results from the `insert`:

Name|Type|Description
----|----|-----------
`inserted`|number|The number of objects inserted into the `Database`
`start`|date|The timestamp of when the insertions began
`end`|date|The timestamp of when the insertions finished
`details`|array|An array of insertion results for each individual `datastore`

### select

Selects data from the `Database`.

Parameters:

Name|Type|Description
----|----|-----------
`selecter`|function|A function that returns a boolean that will be used to identify the records that should be returned

Resolves with an object containing results from the `select`:

Name|Type|Description
----|----|-----------
`data`|array|An array of objects selected from the `Database`
`selected`|number|The number of objects selected from the `Database`
`ignored`|number|The number of objects that were not selected from the `Database`
`start`|date|The timestamp of when the insertions began
`end`|date|The timestamp of when the insertions finished
`details`|array|An array of selection results for each individual `datastore`

### update

Updates data in the `Database`.

Parameters:

Name|Type|Description
----|----|-----------
`selecter`|function|A function that returns a boolean that will be used to identify the records that should be updated
`updater`|function|A function that applies an update to a selected record and returns it

Resolves with an object containing results from the `update`:

Name|Type|Description
----|----|-----------
`updated`|number|The number of objects updated in the `Database`
`unchanged`|number|The number of objects that were not updated in the `Database`
`start`|date|The timestamp of when the insertions began
`end`|date|The timestamp of when the insertions finished
`details`|array|An array of update results for each individual `datastore`

### delete

Deletes data from the `Database`.

Parameters:

Name|Type|Description
----|----|-----------
`selecter`|function|A function that returns a boolean that will be used to identify the records that should be deleted

Resolves with an object containing results from the `delete`:

Name|Type|Description
----|----|-----------
`deleted`|number|The number of objects deleted from the `Database`
`retained`|number|The number of objects that were not deleted from the `Database`
`start`|date|The timestamp of when the insertions began
`end`|date|The timestamp of when the insertions finished
`details`|array|An array of deletion results for each individual `datastore`


