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
import log from './Logger';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Sets sequence value.
 */
export async function setSequenceValue(conversion: Conversion, tableName: string): Promise<void> {
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const autoIncrementedColumn: any = conversion._dicTables[tableName].arrTableColumns.find((column: any) => column.Extra === 'auto_increment');
    const dbAccess: DBAccess = new DBAccess(conversion);
    const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, autoIncrementedColumn.Field, false);
    const seqName: string = `${ tableName }_${ columnName }_seq`;
    const sql: string = `SELECT SETVAL(\'"${ conversion._schema }"."${ seqName }"\', 
                (SELECT MAX("' + columnName + '") FROM "${ conversion._schema }"."${ tableName }"));`;

    await dbAccess.query('SequencesProcessor::setSequenceValue', sql, DBVendors.PG, false, false);
    const successMsg: string = `\t--[setSequenceValue] Sequence "${ conversion._schema }"."${ seqName }" is created...`;
    log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
}

/**
 * Defines which column in given table has the "auto_increment" attribute.
 * Creates an appropriate sequence.
 */
export async function createSequence(conversion: Conversion, tableName: string): Promise<void> {
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const autoIncrementedColumn: any = conversion._dicTables[tableName].arrTableColumns.find((column: any) => column.Extra === 'auto_increment');
    const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, autoIncrementedColumn.Field, false);
    const logTitle: string = 'SequencesProcessor::createSequence';
    const dbAccess: DBAccess = new DBAccess(conversion);
    const seqName: string = `${ tableName }_${ columnName }_seq`;

    const sqlCreateSequence: string = `CREATE SEQUENCE "${ conversion._schema }"."${ seqName }";`;
    const createSequenceResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlCreateSequence, DBVendors.PG, false, true);

    const sqlSetNextVal: string = `ALTER TABLE "${ conversion._schema }"."${ tableName }" ALTER COLUMN "${ columnName }" 
        SET DEFAULT NEXTVAL("${ conversion._schema }"."${ seqName }");`;

    const setNextValResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlSetNextVal, DBVendors.PG, false, true, createSequenceResult.client);

    const sqlSetSequenceOwner: string = `ALTER SEQUENCE "${ conversion._schema }"."${ seqName }"
        OWNED BY "${ conversion._schema }"."${ tableName }"."${ columnName }";`;

    const setSequenceOwnerResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlSetSequenceOwner, DBVendors.PG, false, true, setNextValResult.client);

    const sqlSetSequenceValue: string = `SELECT SETVAL(\'"${ conversion._schema }"."${ seqName }"\', 
        (SELECT MAX("' + columnName + '") FROM "${ conversion._schema }"."${ tableName }"));`;

    await dbAccess.query(logTitle, sqlSetSequenceValue, DBVendors.PG, false, false, setSequenceOwnerResult.client);

    const successMsg: string = `\t--[${ logTitle }] Sequence "${ conversion._schema }"."${ seqName }" is created...`;
    log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
}
