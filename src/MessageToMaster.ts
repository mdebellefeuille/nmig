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
export default class MessageToMaster {
    /**
     * A name of a table, to insert the data into.
     */
    public readonly tableName: string;

    /**
     * A number of rows, that have already been inserted into given table.
     */
    public rowsInserted: number;

    /**
     * A number of rows to insert into given table.
     */
    public readonly totalRowsToInsert: number;

    /**
     * Representation of a message of DataLoader process to the master process regarding records,
     * inserted to specified table.
     */
    public constructor(tableName: string, rowsInserted: number, totalRowsToInsert: number) {
        this.tableName = tableName;
        this.rowsInserted = rowsInserted;
        this.totalRowsToInsert = totalRowsToInsert;
    }
}
