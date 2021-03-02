# njodb

`njodb` is a persistent, partitioned, concurrency-controlled, Node.js JSON object database. Data is written to the file system and distributed across multiple files that are protected by read and write locks.

## Table of contents
- [Install](#install)
- [Test](#test)
- [Introduction](#introduction)
- [Constructor](#constructor)
  - [Database properties](#database-properties)
- [Database management methods](#database-management-methods)
  - [grow](#grow)
  - [shrink](#shrink)
  - [resize](#resize)
  - [drop](#drop)
  - [getStats](#getStats)
  - [getProperties](#getproperties)
  - [setProperties](#setproperties)
- [Data manipulation methods](#data-manipulation-methods)
  - [insert](#insert)
  - [select](#select)
  - [aggregate](#aggregate)
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
db.insert(data).then( /* do something */ );
```

Select some records from the database by supplying a function to find matches:
```js
db.select(function(record) { return record.id === 1 || record.name === "Steve"}).then( /* do something */ );
```

Update some records in the database by supplying a function to find matches and another function to update them:
```js
db.update(function(record) { return record.name === "James"}, function(record) { record.nickname = "Bulldog"; return record; }).then( /* do something */ );
```

Delete some records from the database by supplying a function to find matches:
```js
db.delete(function(record) { return record.modified < Date.now()}).then( /* do something */ );
```

Delete the database:
```js
db.drop().then( /* do something */ );
```

## Constructor
Creates a new instance of an NJODB `Database`.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`root`|string|path to the root directory of the `Database`|`process.cwd()`

If an `njodb.properties` file already exists in the `root` directory, a connection to the existing `Database` will be created. If the `root` directory does not exist it will be created, along with a default `njodb.properties` file (see [Database properties](#database-properties) below). If the data and temp directories do not exist, they will be created.

Example:

```js
const db = new njodb.Database() // created in or connected to the current directory

const db = new njodb.Database("/path/to/some/other/place") // created or connected to elsewhere
```

### Database properties
An NJODB `Database` has several properties that control its functioning. These properties can be set explicitly in the `njodb.properties` file in the `root` directory; otherwise, default properties will be used. For a newly created `Database`, an `njodb.properties` file will be created using default values.

Properties:

Name|Type|Description|Default
----|----|-----------|-------
`datadir`|string|The name of the subdirectory of `root` where data files will be stored|`data`
`dataname`|string|The file name that will be used when creating or accessing data files|`data`
`datastores`|number|The number of data partitions that will be used|`5`
`tempdir`|string|The name of the subdirectory of `root` where temporary data files will be stored|`tmp`
`lockoptions`|object|The options that will be used by [proper-lockfile](https://www.npmjs.com/package/proper-lockfile) to lock data files|`{"stale": 5000, "update": 1000, "retries": { "retries": 5000, "minTimeout": 250, "maxTimeout": 5000, "factor": 0.15, "randomize": false } }`
`debug`|boolean|Whether to print out debugging information to the console|`false`


## Database management methods

### grow

`grow()`

An asynchronous method that increases the number of `datastores` by one and redistributes the data across them.

### shrink

`shrink()`

An asynchronous method that decreases the number of `datastores` by one and redistributes the data across them. If the current number of `datastores` is one, calling `shrink()` will throw an error.

### resize

`resize(size)`

An asynchronous method that changes the number of `datastores` and redistributes the data across them.

Parameters:

Name|Type|Description
----|----|-----------
`size`|number|The number of `datastores` (must be greater than zero)

### drop

`drop()`

An asynchronous method that deletes the database, including the data and temp directories, and the properties file.

### getStats

`getStats()`

An asynchronous method that returns statistics about the `Database`.

Returns a promises that resolves with the following information:

Name|Description
----|-----------
`size`|The total size of the `Database` (the sum of the sizes of the individual `datastores`)
`stores`|The total number of `datastores` in the `Database`
`records`|The total number of records in the `Database` (the sum of the number of records in each `datastore`)
`min`|The minimum number of records in a `datastore`
`max`|The maximum number of records in a `datastore`
`mean`|The mean (i.e., average) number of records in each `datastore`
`var`|The variance of the number of records across `datastores`
`std`|The standard deviation of the number of records across `datastores`
`start`|The timestamp of when the `getStats` call started
`end`|The timestamp of when the `getStats` call finished
`details`|An array of detailed stats for each `datastore`

### getProperties

`getProperties()`

Returns the properties set for the `Database`.

### setProperties

`setProperties(properties)`

Sets the properties for the the `Database`.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`properties`|object|The properties to set for the `Database`|See [Database properties](#database-properties)


## Data manipulation methods

All data manipulation methods are asynchronous and return a `Promise`.

### insert

`insert(data)`

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

`select(selecter [, projector])`

Selects data from the `Database`.

Parameters:

Name|Type|Description
----|----|-----------
`selecter`|function|A function that returns a boolean that will be used to identify the records that should be returned
`projecter`|function| A function that returns an object that identifies the fields that should be returned

Resolves with an object containing results from the `select`:

Name|Type|Description
----|----|-----------
`data`|array|An array of objects selected from the `Database`
`selected`|number|The number of objects selected from the `Database`
`ignored`|number|The number of objects that were not selected from the `Database`
`start`|date|The timestamp of when the selections began
`end`|date|The timestamp of when the selections finished
`details`|array|An array of selection results for each individual `datastore`

Example with projection (returns only the `id` and `modified` fields but also creates a new one called `newID`):

```js
db.select(
    function () { return true; }, // selecter (return all records)
    function (record) { return {id: record.id, newID: record.id + 1, modified: record.modified }} // projecter (return a subset of fields and create a new one)
);
```

### aggregate

`aggregate(selecter, indexer [, projecter])`

Aggregates data in the database.

Parameters:

Name|Type|Description
----|----|-----------
`selecter`|function|A function that returns a boolean that will be used to identify the records that should be aggregated
`indexer`|function| A function that returns an object that creates the index by which data will be grouped
`projecter`|function| A function that returns an object that identifies the fields that should be returned

Resolves with an object containing results from the `aggregate`:

Name|Type|Description
----|----|-----------
`data`|array|An array of index objects selected from the `Database`
`start`|date|The timestamp of when the aggregations began
`end`|date|The timestamp of when the aggregations finished
`details`|array|An array of selection results for each individual `datastore`

Each index object contains the following:

Name|Type|Description
----|----|-----------
`index`|any valid type|The value of the index created by the indexer function
`data`|array|An array of aggregation objects for each field of the records returned

Each aggregation object contains the following:

Name|Description
----|-----------
`min`|Minimum value of the field
`max`|Maximum value of the field
`count`|The count of records that matched the index
`sum`|The sum of the values of the field (undefined if not a number)
`mean`| The mean (i.e., average) of the values of the field (undefined if not a number)
`varp`|The population variance of the values of the field (undefined if not a number)
`vars`|The sample variance of the values of the field (undefined if not a number)
`stdp`|The population standard deviation of the values of the field (undefined if not a number)
`stds`|The sample standard deviation of the values of the field (undefined if not a number)

An example (returns aggregates for all records and fields, grouped by state and lastName fields):
```js
db.aggregate(
    function () { return true; }, // selecter (use all records)
    function (record) { return [record.state, record.lastName]; } // indexer (group records by state and lastName)
);
```

Another example (returns aggregates for only two fields, grouped by state):
```js
db.aggregate(
    function (record) { return record.id < 1000; }, // selecter (only use records with an ID less than 1000)
    function (record) { return [record.state]; }, // indexer (group by state)
    function (record) { return {favoriteNumber: record.favoriteNumber, firstName: record.firstName}; } // projecter (only return aggregate data for two fields)
);
```

Example aggregate data array:
```js
[
    {
        index: "Maryland",
        aggregates: [
            {
                field: "favoriteNumber",
                data: {
                      min: 0,
                      max: 98,
                      count: 50,
                      sum: 2450,
                      mean: 49,
                      varp: 833,
                      vars: 850,
                      stdp: 28.861739379323623,
                      stds: 29.154759474226502
                  }
            },
            {
                field: "firstName",
                data: {
                    min: "Elizabeth",
                    max: "William",
                    count: 50,
                    sum: undefined,
                    mean: undefined,
                    varp: undefined,
                    vars: undefined,
                    stdp: undefined,
                    stds: undefined
                }
            }
        ]
    },
    {
        index: "Virginia",
        aggregates: [
            {
                field: "favoriteNumber",
                data: {
                    min: 0,
                    max: 49,
                    count: 50,
                    sum: 1225,
                    mean: 24.5,
                    varp: 208.25000000000003,
                    vars: 212.50000000000003,
                    stdp: 14.430869689661813,
                    stds: 14.577379737113253
                }
            },
            {
                field: "firstName",
                data: {
                    min: "James",
                    max: "Robert",
                    count: 50,
                    sum: undefined,
                    mean: undefined,
                    varp: undefined,
                    vars: undefined,
                    stdp: undefined,
                    stds: undefined
                }
            }
        ]
    }
]
```


### update

`update(selecter, updater)`

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
`start`|date|The timestamp of when the updates began
`end`|date|The timestamp of when the updates finished
`details`|array|An array of update results for each individual `datastore`

### delete

`delete(selecter)`

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
`start`|date|The timestamp of when the deletions began
`end`|date|The timestamp of when the deletions finished
`details`|array|An array of deletion results for each individual `datastore`


