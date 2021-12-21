export class Database {
    /** 
    * Creates a new instance of an NJODB Database.
    * @description If an njodb.properties file already exists in the root directory, a connection to the 
    * existing Database will be created. If the root directory does not exist it will be created. 
    * If no user-specific properties are supplied, an njodb.properties file will be created using 
    * default values; otherwise, the user-supplied properties will be merged with the default values 
    * (click the link below to see Database properties). If the data and temp directories do not exist, they will be created.
    * @link https://github.com/jamesbontempo/njodb#database-properties
    * @example
    * const db = new njodb.Database() // created in or connected to the current directory
      const db = new njodb.Database("/path/to/some/other/place", {datadir: "mydata", datastores: 2}) // created or connected to elsewhere with user-supplied properties
    */
    constructor(root: string, properties?: NJODBProperties);
    /**
     * Insert into the database.
     * @param data - An array of JSON objects to insert into the Database
     * @example
     * db.insert(data).then(results => do something );
     */
    insert(data: any[]): Promise<InsertStat>;
       /**
     * Insert into the database.
     * @param data - An array of JSON objects to insert into the Database
     * @example
     * db.insertSync(data)
     */
    insertSync(data: any[]): InsertStat
    /**
     * Inserts data into the database from a file containing JSON data. 
     * The file itself does not need to be a valid JSON object, rather it should contain a single stringified JSON object per line. 
     * Blank lines are ignored and problematic data is collected in an errors array.
     * Resolves with an object containing results from the insertFile.
     * @example
     * db.insertFile("./test/data.json").then(results =>  do something );
     */
    insertFile(filePath: string): Promise<InsertFileStat>
     /**
     * Inserts data into the database from a file containing JSON data. 
     * The file itself does not need to be a valid JSON object, rather it should contain a single stringified JSON object per line. 
     * Blank lines are ignored and problematic data is collected in an errors array.
     * Resolves with an object containing results from the insertFile.
     * @example
     * db.insertFileSync("./test/data.json")
     */
    insertFileSync(filePath: string): InsertFileStat
    /**
     * Select some records from the database by supplying a function to find matches.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be returned
     * @param projector - A function that returns an object that identifies the fields that should be returned
     * @example
     * db.select(
     *      record => record.id === 1 || record.name === "Steve"
     * ).then(results => do something );
     * 
     * db.select(
            () => true,
            record => { return {id: record.id, newID: record.id + 1, modified: record.modified }; }
       ).then(results => do something );
     */
    select(selecter: (record: any) => boolean,projector: (record:any)=>any): Promise<SelectStat>;
     /**
     * Select some records from the database by supplying a function to find matches.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be returned
     * @param projector - A function that returns an object that identifies the fields that should be returned
     * @example
     * let records = db.selectSync(
     *      record => record.id === 1 || record.name === "Steve"
     * )
     * 
     * let anoterRecords = db.selectSync(
            () => true,
            record => { return {id: record.id, newID: record.id + 1, modified: record.modified }; }
       );
     */
    selectSync(selecter: (record: any) => boolean,projector: (record:any)=>any ): SelectStat
    /**
     * Update some records in the database
     * by supplying a function to find matches and another function to update them.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be updated
     * @param updater - A function that applies an update to a selected record and returns it
     * @example
     * db.update(
            record => record.name === "James",
            record => { record.nickname = "Bulldog"; return record; }
       ).then(results =>  do something );
     */
    update(selecter: (record: any) => boolean, updater: (record: any)=>any): Promise<UpdateStat>;
     /**
     * Update some records in the database
     * by supplying a function to find matches and another function to update them.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be updated
     * @param updater - A function that applies an update to a selected record and returns it
     * @example
     * db.updateSync(
            record => record.name === "James",
            record => { record.nickname = "Bulldog"; return record; }
       )
     */
    updateSync(selecter: (record: any) => boolean, updater: (record: any)=>any) : UpdateStat
    /**
     * Deletes data from the Database.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be deleted
     * @example
     * db.delete(
            record => record.modified < Date.now()
       ).then(results =>  do something  );
     */
    delete(selecter: (record: any) => boolean): Promise<DeleteStat>;
     /**
     * Deletes data from the Database.
     * @param selecter - A function that returns a boolean that will be used to identify the records that should be deleted
     * @example
     * db.deleteSync(
            record => record.modified < Date.now()
       )
     */
    deleteSync(selecter: (record: any) => boolean) : DeleteStat
    /**
     * Delete the database.
     * db.drop().then(results => do something );
     */
    drop(): Promise<unknown>;
     /** A synchronous version of drop. */
    dropSync(): void;
    /**
     * Returns statistics about the Database.
     */
    stats(): Promise<Stats>;
    /**
     * A synchronous version of stats.
     */
    statsSync(): Stats;
    /**
     * Increases the number of datastores by one and redistributes the data across them.
     */
    grow(): Promise<any>;
    /**
     * A synchronous version of grow.
     */
    growSync(): any;
    /**
     * Decreases the number of datastores by one and redistributes the data across them.
     * If the current number of datastores is one, calling shrink() will throw an error.
     */
    shrink(): Promise<any>;
    /**
     * A synchronous version of shrink.
     */
    shrinkSync(): any;
    /** Changes the number of datastores and redistributes the data across them. */
    resize(size: number): Promise<any>;
    /**
     * A synchronous version of resize.
     */
    resizeSync(size: number): any;
    /**
     * Returns the properties set for the Database. Will likely be deprecated in a future version.
     * @warning Will likely be deprecated in a future version.
     */
    getProperties(): Promise<any>;
    /**
     * Sets the properties for the the Database. Will likely be deprecated in a future version.
     * @warning Will likely be deprecated in a future version.
     */
     setProperties(properties: NJODBProperties): Promise<any>;
     /**
      * Aggregates data in the database.
      * @param selecter - A function that returns a boolean that will be used to identify the records that should be aggregated
      * @param indexer - A function that returns an object that creates the index by which data will be grouped
      * @param projecter - A function that returns an object that identifies the fields that should be returned
      * @example
      * //An example that generates aggregates for all records and fields, grouped by state and lastName:
      * db.aggregate(
        () => true,
            record => [record.state, record.lastName]
        );
        //Another example that generates aggregates for records with an ID less than 1000, grouped by state, but for only two fields (note the non-numeric fields do not include numeric aggregate data):
        db.aggregate(
            record => record.id < 1000,
            record => record.state,
            record => { return {favoriteNumber: record.favoriteNumber, firstName: record.firstName}; }
        );
      */
     aggregate(selecter: (record:any)=>boolean,indexer: (record:any)=>any,projecter: (record:any)=>any): Promise<AggregateResult>
     /**
      * Aggregates data in the database.
      * @param selecter - A function that returns a boolean that will be used to identify the records that should be aggregated
      * @param indexer - A function that returns an object that creates the index by which data will be grouped
      * @param projecter - A function that returns an object that identifies the fields that should be returned
      * @example
      * //An example that generates aggregates for all records and fields, grouped by state and lastName:
      * db.aggregateSync(
        () => true,
            record => [record.state, record.lastName]
        );
        //Another example that generates aggregates for records with an ID less than 1000, grouped by state, but for only two fields (note the non-numeric fields do not include numeric aggregate data):
        db.aggregateSync(
            record => record.id < 1000,
            record => record.state,
            record => { return {favoriteNumber: record.favoriteNumber, firstName: record.firstName}; }
        );
      */
    aggregateSync(selecter: (record:any)=>boolean,indexer: (record:any)=>any,projecter: (record:any)=>any): AggregateResult
}

export type Stats = {
    /** The path of the root directory of the Database */
    root: string;
    /** The path of the data subdirectory of the Database */
    data: string;
    /** The path of the temp subdirectory of the Database */
    temp: string;
    /** The number of records in the Database (the sum of the number of records in each datastore) */
    records: number;
    /** The number of problematic records in the Database */
    errors: any;
    /** The total size of the Database in "human-readable" format (the sum of the sizes of the individual datastores) */
    size: string;
    /** The total number of datastores in the Database */
    stores: number;
    /** The minimum number of records in a datastore */
    min: number;
    /** The maximum number of records in a datastore */
    max: number;
    /** The mean (i.e., average) number of records in each datastore */
    mean: number;
    /** The variance of the number of records across datastores */
    var: any;
    /** The standard deviation of the number of records across datastores */
    std: any;
    /** The timestamp of when the stats call started */
    start: any;
    /** The timestamp of when the stats call finished */
    end: any;
    /** The amount of time in milliseconds required to run the stats call */
    elapsed: any;
    /** An array of detailed stats for each datastore */
    details: any;
};

export interface NJODBProperties {
    datadir?: string;
    dataname?: string;
    datastores?: number;
    tempdir?: string;
    lockoptions: any;
}

export type InsertStat = {
    /** The number of lines of the file inspected */
    inserted: number,
    /** The timestamp of when the insertions began */
    start: Date,
    /** The timestamp of when the insertions finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the insert */
    elapsed: number,
    /** An array of insertion results for each individual datastore */
    details: any[]
}

export type InsertFileStat = {
    /** The number of lines of the file inspected */
    inspected: number,
    /** The number of objects inserted into the Database */
    inserted: number,
    /** The number of blank lines in the file */
    blanks: number,
    /** An array of problematic records in the file */
    errors: any[],
    /** The timestamp of when the insertions began */
    start: Date,
    /** The timestamp of when the insertions finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the insert */
    elapsed: number,
    /** An array of insertion results for each individual datastore */
    details: any[]
}

export type SelectStat = {
    /** An array of objects selected from the Database */
    data: any[],
    /** The number of objects selected from the Database */
    selected: number,
    /** The number of objects that were not selected from the Database */
    ignored: number,
    /** An array of problematic (i.e., un-parseable) records in the Database */
    errors: any[],
    /** The timestamp of when the selections began */
    start: Date,
    /** The timestamp of when the selections finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the select */
    elapsed: number,
    /** An array of selection results, including error details, for each individual datastore */
    details: any[]
}

export type UpdateStat = {
    /** The number of objects updated in the Database */
    updated: number,
    /** The number of objects that were not updated in the Database */
    unchanged: number,
    /** An array of problematic (i.e., un-parseable) records in the Database or records that were unable to be updated */
    errors: any[],
    /** The timestamp of when the updates began */
    start: Date,
    /** The timestamp of when the updates finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the update */
    elapsed: number,
    /** An array of update results, including error details, for each individual datastore */
    details: any[]
}

export type DeleteStat = {
    /** The number of objects deleted from the Database */
    deleted: number,
    /** The number of objects that were not deleted from the Database */
    retained: number,
    /** An array of problematic (i.e., un-parseable) records in the Database or records that were unable to be deleted */
    errors: any[],
    /** 	The timestamp of when the deletions began */
    start: Date,
    /** The timestamp of when the deletions finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the delete */
    elapsed: number,
    /** An array of deletion results, including error details, for each individual datastore */
    details: any[]
}

export type AggregateResult = {
    /** An array of index objects selected from the Database */
    data: {
        /** The value of the index created by the indexer function */
        index: any,
        /** The count of records that contained the index */
        count: number,
        /** An array of aggregation objects for each field of the records returned */
        data: {
            /** Minimum value of the field */
            min: any,
            /** Maximum value of the field */
            max: any,
            /** The sum of the values of the field (undefined if not a number) */
            sum: number | undefined,
            /** The mean (i.e., average) of the values of the field (undefined if not a number) */
            mean: number | undefined,
            /** The population variance of the values of the field (undefined if not a number) */
            varp: number | undefined,
            /** The sample variance of the values of the field (undefined if not a number) */
            vars: number | undefined,
            /** The population standard deviation of the values of the field (undefined if not a number) */
            stdp: number | undefined,
            /** The sample standard deviation of the values of the field (undefined if not a number) */
            stds: number | undefined,
        }[]
    }[],
    /** The number of records that were indexable (i.e., processable by the indexer function) */
    indexed: number,
    /** The number of records that were un-indexable */
    unindexed: number,
    /** The number of problematic (i.e., un-parseable) records in the DatabaseThe number of problematic (i.e., un-parseable) records in the Database */
    errors: number,
    /** The timestamp of when the aggregations began */
    start: Date,
    /** The timestamp of when the aggregations finished */
    end: Date,
    /** The amount of time in milliseconds required to execute the aggregate */
    elapsed: number,
    /** An array of selection results, including error details, for each individual datastore */
    details: any[]
}
