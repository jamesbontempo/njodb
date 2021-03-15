# njodb

`njodb` is a persistent, partitioned, concurrency-controlled, Node.js JSON object database. Data is written to the file system and distributed across multiple files that are protected by read and write locks. By default, all methods are asynchronous and use read/write streams to improve performance and reduce memory requirements (this should be particularly useful for large databases).

`njodb 0.4.0` introduced synchronous versions of the methods since, depending on the context, they could be more relevant or useful. This version also included the `grow`, `shrink`, and `resize` methods for easily adjusting the number of partitions and redistributing data across them.

`njodb 0.4.3` introduced the `insertFile` method for easily importing JSON data from a file into the database.

## Table of contents
- [Install](#install)
- [Test](#test)
- [Introduction](#introduction)
- [Constructor](#constructor)
  - [Database properties](#database-properties)
- [Database management methods](#database-management-methods)
  - [stats](#stats) / [statsSync](#statsSync)
  - [grow](#grow) / [growSync](#growSync)
  - [shrink](#shrink) / [shrinkSync](#shrinkSync)
  - [resize](#resize) / [resizeSync](#resizeSync)
  - [drop](#drop) / [dropSync](#dropSync)
  - [getProperties](#getproperties) / [setProperties](#setproperties)
- [Data manipulation methods](#data-manipulation-methods)
  - [insert](#insert) / [insertSync](#insertSync)
  - [insertFile](#insertFile) / [insertFileSync](#insertFileSync)
  - [select](#select) / [selectSync](#selectSync)
  - [aggregate](#aggregate) / [aggregateSync](#aggregateSync)
  - [update](#update) / [updateSync](#updateSync)
  - [delete](#delete) / [deleteSync](#deleteSync)
- [Finding and fixing problematic data](#finding-and-fixing-problematic-data)

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
db.select(
    record => record.id === 1 || record.name === "Steve"
).then( /* do something */ );
```

Update some records in the database by supplying a function to find matches and another function to update them:
```js
db.update(
    record => record.name === "James",
    record => { record.nickname = "Bulldog"; return record; }
).then( /* do something */ );
```

Delete some records from the database by supplying a function to find matches:
```js
db.delete(
    record => record.modified < Date.now()
).then( /* do something */ );
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


## Database management methods

### stats

`stats`

Returns statistics about the `Database`. Resolves with the following information:

Name|Description
----|-----------
`root`|The path of the root directory of the `Database`
`data`|The path of the data subdirectory of the `Database`
`temp`|The path of the temp subdirectory of the `Database`
`records`|The number of records in the `Database` (the sum of the number of records in each `datastore`)
`errors`|The number of problematic records in the `Database`
`size`|The total size of the `Database` in "human-readable" format (the sum of the sizes of the individual `datastores`)
`stores`|The total number of `datastores` in the `Database`
`min`|The minimum number of records in a `datastore`
`max`|The maximum number of records in a `datastore`
`mean`|The mean (i.e., average) number of records in each `datastore`
`var`|The variance of the number of records across `datastores`
`std`|The standard deviation of the number of records across `datastores`
`start`|The timestamp of when the `stats` call started
`end`|The timestamp of when the `stats` call finished
`elapsed`|The amount of time in milliseconds required to run the `stats` call
`details`|An array of detailed stats for each `datastore`

### statsSync

A synchronous version of `stats`.

### grow

`grow()`

Increases the number of `datastores` by one and redistributes the data across them.

### growSync

`growSync()`

A synchronous version of `grow`.

### shrink

`shrink()`

Decreases the number of `datastores` by one and redistributes the data across them. If the current number of `datastores` is one, calling `shrink()` will throw an error.

### shrinkSync

`shrinkSync()`

A synchronous version of `shrink`.

### resize

`resize(size)`

Changes the number of `datastores` and redistributes the data across them.

Parameters:

Name|Type|Description
----|----|-----------
`size`|number|The number of `datastores` (must be greater than zero)

### resizeSync

`resizeSync(size)`

A synchronous version of `resize`.

### drop

`drop()`

Deletes the database, including the data and temp directories, and the properties file.

### dropSync

`dropSync()`

A synchronous version of `drop`.

### getStats

`getStats()`

DEPRECATED: Currently an alias for `stats`. Will likely be dropped in a future version.

### getStatsSync

`getStatsSync()`

DEPRECATED: Currently an alias for `statsSync`. Will likely be dropped in a future version.

### getProperties

`getProperties()`

Returns the properties set for the `Database`. Will likely be deprecated in a future version.

### setProperties

`setProperties(properties)`

Sets the properties for the the `Database`. Will likely be deprecated in a future version.

Parameters:

Name|Type|Description|Default
----|----|-----------|-------
`properties`|object|The properties to set for the `Database`|See [Database properties](#database-properties)


## Data manipulation methods

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
`elapsed`|number|The amount of time in milliseconds required to execute the `insert`
`details`|array|An array of insertion results for each individual `datastore`

### insertSync

`insertSync(data)`

A synchronous version of `insert`.

### insertFile

`insertFile(file)`

Inserts data into the `database` from a file containing JSON data. The file itself does not need to be a valid JSON object, rather it should contain a single stringified JSON object per line. Blank lines are ignored and problematic data is collected in an `errors` array.

Resolves with an object containing results from the `insertFile`:

Name|Type|Description
----|----|-----------
`inspected`|number|The number of lines of the file inspected
`inserted`|number|The number of objects inserted into the `Database`
`blanks`|number|The number of blank lines in the file
`errors`|array|An array of problematic records in the file
`start`|date|The timestamp of when the insertions began
`end`|date|The timestamp of when the insertions finished
`elapsed`|number|The amount of time in milliseconds required to execute the `insert`
`details`|array|An array of insertion results for each individual `datastore`

An example data file, `data.json`, is included in the `test` subdirectory. Among many valid records, it also includes blank lines and a malformed JSON object.

### insertFileSync

`insertFileSync(file)`

A synchronous version of `insertFile`.

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
`errors`|number|The number of problematic (i.e., un-parseable) records in the `Database`
`start`|date|The timestamp of when the selections began
`end`|date|The timestamp of when the selections finished
`elapsed`|number|The amount of time in milliseconds required to execute the `select`
`details`|array|An array of selection results, including error details, for each individual `datastore`

Example with projection that selects all records, returns only the `id` and `modified` fields, but also creates a new one called `newID`:

```js
db.select(
    () => true,
    record => { return {id: record.id, newID: record.id + 1, modified: record.modified }; }
);
```

### selectSync

`selectSync(selecter [, projector])`

A synchronous version of `select`.

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
`indexed`|number|The number of records that were indexable (i.e., processable by the indexer function)
`unindexed`|number|The number of records that were un-indexable
`errors`|number|The number of problematic (i.e., un-parseable) records in the `Database`
`start`|date|The timestamp of when the aggregations began
`end`|date|The timestamp of when the aggregations finished
`elapsed`|number|The amount of time in milliseconds required to execute the `aggregate`
`details`|array|An array of selection results, including error details, for each individual `datastore`

Each index object contains the following:

Name|Type|Description
----|----|-----------
`index`|any valid type|The value of the index created by the indexer function
`count`|number|The count of records that contained the index
`data`|array|An array of aggregation objects for each field of the records returned

Each aggregation object contains one or more of the following (non-numeric fields do not contain numeric aggregate data):

Name|Type|Description
----|----|-----------
`min`|any valid type|Minimum value of the field
`max`|any valid type|Maximum value of the field
`sum`|number|The sum of the values of the field (undefined if not a number)
`mean`|number|The mean (i.e., average) of the values of the field (undefined if not a number)
`varp`|number|The population variance of the values of the field (undefined if not a number)
`vars`|number|The sample variance of the values of the field (undefined if not a number)
`stdp`|number|The population standard deviation of the values of the field (undefined if not a number)
`stds`|number|The sample standard deviation of the values of the field (undefined if not a number)

An example that generates aggregates for all records and fields, grouped by state and lastName:
```js
db.aggregate(
    () => true,
    record => [record.state, record.lastName]
);
```

Another example that generates aggregates for records with an ID less than 1000, grouped by state, but for only two fields (note the non-numeric fields do not include numeric aggregate data):
```js
db.aggregate(
    record => record.id < 1000,
    record => record.state,
    record => { return {favoriteNumber: record.favoriteNumber, firstName: record.firstName}; }
);
```

Example aggregate data array:
```js
[
    {
        index: "Maryland",
        count: 50,
        aggregates: [
            {
                field: "favoriteNumber",
                data: {
                      min: 0,
                      max: 98,
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
                    max: "William"
                }
            }
        ]
    },
    {
        index: "Virginia",
        count: 50,
        aggregates: [
            {
                field: "favoriteNumber",
                data: {
                    min: 0,
                    max: 49,
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
                    count: 50
                }
            }
        ]
    }
]
```

### aggregateSync

`aggregate(selecter, indexer [, projecter])`

A synchronous version of `aggregate`.

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
`errors`|number|The number of problematic (i.e., un-parseable) records in the `Database`
`start`|date|The timestamp of when the updates began
`end`|date|The timestamp of when the updates finished
`elapsed`|number|The amount of time in milliseconds required to execute the `update`
`details`|array|An array of update results, including error details, for each individual `datastore`

### updateSync

`updateSync(selecter, updater)`

A synchronous version of `update`

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
`errors`|number|The number of problematic (i.e., un-parseable) records in the `Database`
`start`|date|The timestamp of when the deletions began
`end`|date|The timestamp of when the deletions finished
`elapsed`|number|The amount of time in milliseconds required to execute the `delete`
`details`|array|An array of deletion results, including error details, for each individual `datastore`

### deleteSync

`deleteSync(selecter)`

A synchronous version of `delete`.


## Finding and fixing problematic data

Many methods return information about problematic records encountered (i.e., records that are not parseable using `JSON.parse()`); both a count of them, as well as details about them in the `details` array. The objects in the `details` array - one for each `datastore` - contain an `errors` array that is a collection of objects about problematic records in the `datastore`. Each error object includes the line of the `datastore` file where the problematic record was found as well as a copy of the record itself. With this information, if one wants to address these problematic data they can simply load the `datastore` file in a text editor and either correct the record or remove it.

Here is an example of the `details` for a `datastore` that contains a problematic record. As you can see, the record is on the first line of the file, and the problem is that the `lastname` key name is missing an enclosing quote. Simply adding the quote fixes the record.

```js
{
    store: '/Users/jamesbontempo/github/njodb/data/data.0.json',
    size: 13362190,
    records: 90157,
    errors: [
      {
        line: 1,
        data: {
          error: '{"id":100,"firstName":"Patricia","lastName:"Brown","state":"California","birthdate":"1980-02-08","favoriteNumbers":[3,3,627],"favoriteNumber":356,"modified":1615242040534}'
        }
      }
    ],
    created: 2021-03-09T01:27:36.431Z,
    modified: 2021-03-09T15:22:22.372Z,
    start: 1615303347030,
    end: 1615303347299
  }
 ```
