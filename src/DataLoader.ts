/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
import * as csvStringify from './CsvStringifyModified';
import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import MessageToMaster from './MessageToMaster';
import { enforceConsistency } from './ConsistencyEnforcer';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import BufferStream from './BufferStream';
import * as path from 'path';
import { PoolClient } from 'pg';
import { PoolConnection } from 'mysql';
import { Transform, TransformOptions, TransformCallback } from 'stream';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".

// process.env.UV_THREADPOOL_SIZE = '64'; // TODO: consider to remove.

process.on('message', async (signal: any) => {
    const conv: Conversion = new Conversion(signal.config);
    log(conv, '\t--[loadData] Loading the data...');

    const promises: Promise<void>[] = signal.chunks.map(async (chunk: any) => {
        const isNormalFlow: boolean = await enforceConsistency(conv, chunk);

        if (isNormalFlow) {
            return populateTableWorker(conv, chunk._tableName, chunk._selectFieldList, chunk._offset, chunk._rowsInChunk, chunk._rowsCnt, chunk._id);
        }

        const dbAccess: DBAccess = new DBAccess(conv);
        const client: PoolClient = await dbAccess.getPgClient();
        return deleteChunk(conv, chunk._id, client);
    });

    await Promise.all(promises);
    processSend('processed');
});

/**
 * Wraps "process.send" method to avoid "cannot invoke an object which is possibly undefined" warning.
 */
function processSend(x: any): void {
    if (process.send) {
        process.send(x);
    }
}

/**
 * Deletes given record from the data-pool.
 * Note, deleteChunk() function is invoked in multiple place, and in some of them,
 * the existing PoolClient instance can be reused.
 */
async function deleteChunk(conv: Conversion, dataPoolId: number, client: PoolClient): Promise<void> {
    const sql: string = `DELETE FROM "${ conv._schema }"."data_pool_${ conv._schema }${ conv._mySqlDbName }" WHERE id = ${ dataPoolId };`;
    const dbAccess: DBAccess = new DBAccess(conv);

    try {
        await client.query(sql);
    } catch (error) {
        await generateError(conv, `\t--[DataLoader::deleteChunk] ${ error }`, sql);
    } finally {
        await dbAccess.releaseDbClient(client);
    }
}

/**
 * Builds a MySQL query to retrieve the chunk of data.
 */
function buildChunkQuery(tableName: string, selectFieldList: string, offset: number, rowsInChunk: number, pkColumnName?: string ): string {
    if (pkColumnName) {
        // TODO: pass column aliases to selectFieldList.
        // return `SELECT ${ selectFieldList } FROM (SELECT ${ pkColumnName } FROM \`${tableName}\` LIMIT ${ offset },${ rowsInChunk })`
           //  + ` AS sub_${ tableName } INNER JOIN ${ tableName } AS main_${ tableName } on main_${ tableName }.id = sub_${ tableName }.id;`;
    }

    return `SELECT ${ selectFieldList } FROM \`${ tableName }\` LIMIT ${ offset },${ rowsInChunk };`;
}

/**
 * Processes data-loading error.
 */
async function processDataError(
    conv: Conversion,
    streamError: string,
    sql: string,
    sqlCopy: string,
    tableName: string,
    dataPoolId: number,
    client: PoolClient
): Promise<void> {
    await generateError(conv, `\t--[populateTableWorker] ${ streamError }`, sqlCopy);
    const rejectedData: string = `\t--[populateTableWorker] Error loading table data:\n${ sql }\n`;
    log(conv, rejectedData, path.join(conv._logsDirPath, `${ tableName }.log`));
    return deleteChunk(conv, dataPoolId, client);
}

/**
 * Loads a chunk of data using "PostgreSQL COPY".
 */
async function populateTableWorker(
    conv: Conversion,
    tableName: string,
    strSelectFieldList: string,
    offset: number,
    rowsInChunk: number,
    rowsCnt: number,
    dataPoolId: number
): Promise<void> {
    return new Promise<void>(async resolvePopulateTableWorker => {
        const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
        const pkColumn: any = null; // conv._dicTables[tableName].arrTableColumns.find((c: any) => c.Key === 'PRI' && c.Extra === 'auto_increment');
        let sql: string = '';

        if (pkColumn) {
            const pkColumnName: string = pkColumn.Field;
            sql = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk, pkColumnName);
        } else {
            sql = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk);
        }

        const dbAccess: DBAccess = new DBAccess(conv);
        const mysqlClient: PoolConnection = await dbAccess.getMysqlClient();

        const sqlCopy: string = `COPY "${ conv._schema }"."${ tableName }" FROM STDIN DELIMITER '${ conv._delimiter }' CSV;`;
        const client: PoolClient = await dbAccess.getPgClient();
        const copyStream: any = client.query(from(sqlCopy));

        copyStream
            .on('end', () => {
                // COPY FROM STDIN does not return the number of rows inserted.
                // But the transactional behavior still applies (no records inserted if at least one failed).
                // That is why in case of 'on end' the rowsInChunk value is actually the number of records inserted.
                processSend(new MessageToMaster(tableName, rowsInChunk, rowsCnt));
                return deleteChunk(conv, dataPoolId, client).then(() => resolvePopulateTableWorker());
            })
            .on('error', (copyStreamError: string) => {
                return processDataError(conv, copyStreamError, sql, sqlCopy, tableName, dataPoolId, client)
                    .then(() => resolvePopulateTableWorker());
            });

        const streamTransform: Transform = new Transform({
            objectMode: true,

            transform(chunk: any, encoding: string, callback: TransformCallback): void {
                chunk[`${ conv._schema }_${ originalTableName }_data_chunk_id_temp`] = dataPoolId; ////////////////

                csvStringify([chunk], async (csvError: any, csvString: string) => {
                    //
                    const buffer: Buffer = Buffer.from(csvString, encoding);
                    this._buffer = Buffer.concat([this._buffer, buffer]);////////////////////////////////////
                    this.push(buffer);
                    callback();
                }, encoding);
            },

            flush(callback: TransformCallback): void {
                callback(undefined, this._buffer);
            }
        });

        mysqlClient
            .query(sql)
            .on('error', (err: string) => {
                // Handle error, an 'end' event will be emitted after this as well
                return processDataError(conv, err, sql, sqlCopy, tableName, dataPoolId, client)
                    .then(() => resolvePopulateTableWorker());
            })
            .on('fields', fields => { // TODO: check what is this good for?
                // the field packets for the rows to follow
            })
            .on('result', row => {
                // Pausing the connnection is useful if your processing involves I/O
                // mysqlClient.pause();

                // processRow(row, function() {
                    // mysqlClient.resume();
                // })
            })
            // .stream({highWaterMark: 5})
            .stream()
            .pipe(streamTransform)
            .pipe(copyStream);

    /* return new Promise<void>(async resolvePopulateTableWorker => {
        const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
        const pkColumn: any = conv._dicTables[tableName].arrTableColumns.find((c: any) => c.Key === 'PRI' && c.Extra === 'auto_increment');
        let sql: string = '';

        if (pkColumn) {
            const pkColumnName: string = pkColumn.Field;
            sql = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk);
        } else {
            sql = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk);
        }

        const dbAccess: DBAccess = new DBAccess(conv);
        const logTitle: string = 'DataLoader::populateTableWorker';
        // console.log(`Before MySQL ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////
        const result: DBAccessQueryResult = await dbAccess.query(logTitle, sql, DBVendors.MYSQL, false, false);
        // console.log(`After MySQL ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////
        if (result.error) {
            return resolvePopulateTableWorker();
        }

        rowsInChunk = result.data.length;
        result.data[0][`${ conv._schema }_${ originalTableName }_data_chunk_id_temp`] = dataPoolId;
        // console.log(`Before CSV ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////
        csvStringify(result.data, async (csvError: any, csvString: string) => {
            if (csvError) {
                await generateError(conv, `\t--[${ logTitle }] ${ csvError }`);
                return resolvePopulateTableWorker();
            }

            const buffer: Buffer = Buffer.from(csvString, conv._encoding);
            // console.log(`After CSV ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////
            const sqlCopy: string = `COPY "${ conv._schema }"."${ tableName }" FROM STDIN DELIMITER '${ conv._delimiter }' CSV;`;
            const client: PoolClient = await dbAccess.getPgClient();
            const copyStream: any = client.query(from(sqlCopy));
            const bufferStream: BufferStream = new BufferStream(buffer);

            copyStream.on('end', () => {
                // console.log(`After COPY ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////

                // COPY FROM STDIN does not return the number of rows inserted.
                // But the transactional behavior still applies (no records inserted if at least one failed).
                // That is why in case of 'on end' the rowsInChunk value is actually the number of records inserted.
                processSend(new MessageToMaster(tableName, rowsInChunk, rowsCnt));
                return deleteChunk(conv, dataPoolId, client).then(() => resolvePopulateTableWorker());
            });

            copyStream.on('error', (copyStreamError: string) => {
                return processDataError(conv, copyStreamError, sql, sqlCopy, tableName, dataPoolId, client)
                    .then(() => resolvePopulateTableWorker());
            });

            bufferStream.on('error', (bufferStreamError: string) => {
                return processDataError(conv, bufferStreamError, sql, sqlCopy, tableName, dataPoolId, client)
                    .then(() => resolvePopulateTableWorker());
            });
            // console.log(`Before COPY ${tableName} ${new Date()}`); // ///////// TODO: remove asap. /////////////////
            bufferStream.setEncoding(conv._encoding).pipe(copyStream);
        }, conv._encoding); */
    });
}
