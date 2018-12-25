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
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".
const { Transform: Json2CsvTransform } = require('json2csv'); // No declaration file for module "json2csv".

// process.env.UV_THREADPOOL_SIZE = '64'; // TODO: consider to remove.

process.on('message', async (signal: any) => {
    const conv: Conversion = new Conversion(signal.config);
    log(conv, '\t--[loadData] Loading the data...');

    const promises: Promise<void>[] = signal.chunks.map(async (chunk: any) => {
        const isNormalFlow: boolean = await enforceConsistency(conv, chunk);

        if (isNormalFlow) {
            return populateTableWorker(
                conv,
                chunk._tableName,
                chunk._selectFieldList,
                chunk._offset,
                chunk._rowsInChunk,
                chunk._rowsCnt,
                chunk._id,
                chunk._pk
            );
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
function buildChunkQuery(
    tableName: string,
    selectFieldList: string,
    offset: number,
    limit: number,
    tableRowsCount?: number,
    pkColumnName?: string | null
): string {
    if (pkColumnName) {
        const pivotPointPassed: boolean = offset > Math.ceil(tableRowsCount! / 2);
        let orderDirection: string = 'ASC';
        let recalculatedOffset: number = offset;

        if (pivotPointPassed) {
            orderDirection = 'DESC';
            recalculatedOffset = tableRowsCount! - offset - limit;
            // A number of rows in the last chunk may be smaller than zero, hence the "negative offset" issue may arise.
            // Following lines handle this issue.
            if (recalculatedOffset < 0) {
                limit += recalculatedOffset;
                recalculatedOffset = 0;
            }
        }

        return `SELECT ${ selectFieldList } FROM (SELECT ${ pkColumnName } FROM \`${ tableName }\``
            + ` ORDER BY ${ pkColumnName } ${ orderDirection } LIMIT ${ recalculatedOffset },${ limit })`
            + ` AS nmig_sub_${ tableName } INNER JOIN ${ tableName } AS ${ tableName } ON ${ tableName }.id = nmig_sub_${ tableName }.id;`;
    }

    return `SELECT ${ selectFieldList } FROM \`${ tableName }\` LIMIT ${ offset },${ limit };`;
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
    dataPoolId: number,
    pkColumn: string | null
): Promise<void> {
    return new Promise<void>(async resolvePopulateTableWorker => {
        const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
        const sql: string = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk, rowsCnt, pkColumn);
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
        }, conv._encoding);

        /*const dbAccess: DBAccess = new DBAccess(conv);
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


        const tableColumnsSql: string = `SHOW COLUMNS FROM ${originalTableName};`;
        const tableColumnsResult: DBAccessQueryResult = await dbAccess.query(
            'DataLoader::populateTableWorker',
            tableColumnsSql,
            DBVendors.MYSQL,
            true,
            false
        );

        const options: any = {
            delimiter: conv._delimiter,
            header: false,
            fields: tableColumnsResult.data.map((column: any) => column.Field)
        };

        const transformOptions: any = {
            highWaterMark: 16384000,
            objectMode: true,
            encoding: conv._encoding
        };

        const json2csvStream = new Json2CsvTransform(options, transformOptions);

        mysqlClient
            .query(sql)
            .on('error', (err: string) => {
                // Handle error, an 'end' event will be emitted after this as well
                return processDataError(conv, err, sql, sqlCopy, tableName, dataPoolId, client)
                    .then(() => resolvePopulateTableWorker());
            })
            .stream({ highWaterMark: 16384000 })
            .pipe(json2csvStream)
            .pipe(copyStream);*/
    });
}
