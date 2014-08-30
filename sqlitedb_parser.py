#!/usr/bin/python
#

# Copyright (C) 2014 n0fate
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# reference : https://github.com/mspreitz/ADEL/blob/master/_sqliteParser.py#L86

#-----------------GLOBALS-------------------
DB_FILE_SIZE_IN_BYTES = 0
# Header bytes [0:15]: sqlite3 magic string "SQLite format 3"
HEADER_MAGIC_STRING = ""
# Header bytes [16:18]: database page size
HEADER_DATABASE_PAGESIZE = 0
# Header bytes [18:19]: file format write version (must be 1 or 2)
HEADER_FILE_FORMAT_WRITE_VERSION = 0
# Header bytes [19:20]: file format read version (must be 1 or 2)
HEADER_FILE_FORMAT_READ_VERSION = None
# Header bytes [20:21]: reserved space per page (usually 0)
HEADER_RESERVED_SPACE_PER_PAGE = None
# Header bytes [21:22]: maximum embedded payload fraction (must be 64)
HEADER_MAXIMUM_EMBEDDED_PAYLOAD_FRACTION = None
# Header bytes [22:23]: minimum embedded payload fraction (must be 32)
HEADER_MINIMUM_EMBEDDED_PAYLOAD_FRACTION = None
# Header bytes [23:24]: leaf payload fraction (must be 32)
HEADER_LEAF_PAYLOAD_FRACTION = None
# Header bytes [24:27]: file change counter
HEADER_FILE_CHANGE_COUNTER = None
# Header bytes [28:31]: database size in pages
HEADER_DATABASE_SIZE_IN_PAGES = None
# Header bytes [32:35]: first freelist trunk page
HEADER_FIRST_FREE_TRUNK_PAGE = None
# Header bytes [36:39]: total number of freelist pages
HEADER_TOTAL_NUMBER_OF_FREELIST_PAGES = None
# Header bytes [40:43]: schema cookie
HEADER_SCHEMA_COOKIE = None
# Header bytes [44:47]: schema format number (must be 1-4)
HEADER_SCHEMA_FORMAT_NUMBER = None
# Header bytes [48:51]: default page cache size
HEADER_DEFAULT_PAGE_CACHE_SIZE = None
# Header bytes [52:55]: largest root b-tree page number
HEADER_LARGEST_ROOT_BTREE_PAGE_NUMBER = None
# Header bytes [56:59]: database text encoding (must be 1-3)
HEADER_DATABASE_TEXT_ENCODING = None
# Header bytes [60:64]: user version
HEADER_USER_VERSION = None
# Header bytes [64:67]: incremental-vacuum mode (1, zero otherwise)
HEADER_INCREMENTAL_VACCUM_MODE = None
# Header bytes [68:91]: reservation for expansion (must be 0)
HEADER_RESERVED_FOR_EXPANSION = None
# Header bytes [92:95]: version valid for number
HEADER_VERSION_VALID_FOR_NUMBER = None
# Header bytes [96:99]: sqlite version number
HEADER_SQLITE_VERSION_NUMBER = None
# Flag indicates whether HEADER_FILE_CHANGE_COUNTER is valid or not
HEADER_FILE_CHANGE_COUNTER_VALID = None
# place holder for integer primary key (= row ID) columns (max 1 per table)
ROW_ID_COLUMN = 0
#-----------------GLOBALS-------------------


# Main function of the sqlite parser scripts. Opens the database, reads the file header,
# parses the database schema definitions and returns a list with exactly one element for
# each database table. Each element holds the complete content of a table, including the
# column definitions of the table in the form of [column name, column type] as first element.
# @file_name:           fully qualified name of the sqlite3 database file to parse
# @return:              list with all contents that were extractet from the database file or
#                       empty list, if an error occured
def parse_db(file_name):
    global DB_FILE_SIZE_IN_BYTES
    global ROW_ID_COLUMN

    # Open the database
    DB_FILE_SIZE_IN_BYTES = _sqliteFileHandler.open_db(file_name)
    if DB_FILE_SIZE_IN_BYTES == 0:
        # file could not be opened correctly
        return []

    # Read first page of database file
    first_page_hex_string = _sqliteFileHandler.read_page(1)
    # ensure that read page could retrieve an existing page
    if (first_page_hex_string == ""):
        return []

    # Parse the database header on the first page (first 100 bytes in the database file)
    parse_db_header(first_page_hex_string)
    if HEADER_DATABASE_TEXT_ENCODING > 1:
        return []

    # Parse database schema (first page of the database file is root b-tree page for the schema btree)
    # Database schema is stored in a well defined way (sqlite master table)
    # CREATE TABLE sqlite_master(
    # type text, # must be one of the following: ['table', 'index', 'view', 'trigger']
    # name text,
    # tbl_name text,
    # rootpage integer,
    # sql text
    # );
    db_schemata = _sqlitePageParser.parse_table_btree_page(first_page_hex_string, 100) # 100 bytes database file header

    # Initialize the resulting content list
    result_list = []
    final_list = []

    # loop through all schemata of the database
    for db_schema in db_schemata:
        if len(db_schema) != 5 + 1: # +1 due to manually added leading rowID
            continue

        # Reset result list for new element
        result_list = []

        # Parse this database element (table, index, view or trigger)
        if (_helpersStringOperations.starts_with_string(str(db_schema[1]), "TABLE") == 0):
            # PARSE TABLE STATEMENT
            # Ensure that we treat a valid schema
            db_schemata_statement = db_schema[len(db_schema) - 1]
            if ((db_schemata_statement == None) or (db_schemata_statement == "")):
                continue

            sql_statement = (db_schema[5]) # db_schema[5] is expected to be the "sql text" as defined in sqlite_master

            # Extract and check command (expected to be CREATE)
            command_tuple = _helpersStringOperations.split_at_first_occurrence(sql_statement, " ")
            if (len(command_tuple) == 0):
                continue
            if (_helpersStringOperations.starts_with_string(str(command_tuple[0]), "CREATE") != 0):
                continue
            # Extract and check first command operand (expected to be TEMP, TEMPORARY, TABLE or VIRTUAL TABLE)
            type_tuple = _helpersStringOperations.split_at_first_occurrence(command_tuple[1], " ")
            if len(type_tuple) == 0:
                continue
            # According to the syntax diagrams of the sqlite SQL create table statement there are TEMP or TEMPORARY key words allowed at this place
            if   (_helpersStringOperations.starts_with_string(str(type_tuple[0]), "TEMP") == 0
              or _helpersStringOperations.starts_with_string(str(type_tuple[0]), "TEMPORARY") == 0
              or _helpersStringOperations.starts_with_string(str(type_tuple[0]), "VIRTUAL") == 0):
                # Ignore and proceed with next fragement (must then be TABLE)
                type_tuple = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], " ")
                if len(type_tuple) == 0:
                    continue
            # This fragment must be table
            if (_helpersStringOperations.starts_with_string(str(type_tuple[0]), "TABLE") != 0):
                continue
            # Extract and check second command operand (expected to be table name)
            name_tuple = []
            next_space = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], " ")
            next_parenthesis = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], "(")
            if (next_space < next_parenthesis):
                # "IF NOT EXISTS" statement possible
                if (_helpersStringOperations.starts_with_string(str(_helpersStringOperations.crop_whitespace(type_tuple[1])), "IF") == 0):
                    type_tuple[1] = type_tuple[1][2:]
                if (_helpersStringOperations.starts_with_string(str(_helpersStringOperations.crop_whitespace(type_tuple[1])), "NOT") == 0):
                    type_tuple[1] = type_tuple[1][3:]
                if (_helpersStringOperations.starts_with_string(str(_helpersStringOperations.crop_whitespace(type_tuple[1])), "EXISTS") == 0):
                    type_tuple[1] = type_tuple[1][6:]
                type_tuple[1] = _helpersStringOperations.crop_whitespace(type_tuple[1])

                # Extract name tuple
                name_tuple = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], " ")
                if len(name_tuple) == 0:
                    name_tuple = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], "(")
                    if len(name_tuple) == 0:
                        continue
                    # Append leading opening parenthesis that we cut off before
                    name_tuple[1] = "(" + str(name_tuple[1])
                else:
                    # "AS ..." statement possible
                    tmp_string = _helpersStringOperations.crop_whitespace(name_tuple[1])
                    if (tmp_string.startswith("AS")):
                        continue
            else:
                name_tuple = _helpersStringOperations.split_at_first_occurrence(type_tuple[1], "(")
                if len(name_tuple) == 0:
                    continue
                # Append leading opening parenthesis that we cut off before
                name_tuple[1] = "(" + str(name_tuple[1])



            # Parse and append sql statement
            name_tuple[1] = _helpersStringOperations.cut_first_last_exclude(name_tuple[1], "(", ")")
            result_list.append(parse_sql_statement_params(name_tuple[1]))

            # Ensure we deal with a real table, virtual tables have no b-tree and thus the b-tree root page pointer is 0
            if (db_schema[4] == 0):
                # Append result from table, index, view or trigger to final list
                final_list.append(result_list)
                continue

            # Parse and append table contents
            btree_root_page_string = _sqliteFileHandler.read_page(db_schema[4])
            # Ensure that read page could retrieve an existing page
            if (btree_root_page_string == ""):
                continue
            table_contents = _sqlitePageParser.parse_table_btree_page(btree_root_page_string, 0)

            # Check whether the table contains a dedicated row ID column
            if (ROW_ID_COLUMN == 0):
                # Table has no dedicated row ID column, add "rowID" to the table statement (the rowID is already extractet)
                index_of_last_element_in_result_list = len(result_list) - 1
                temp_list = result_list[index_of_last_element_in_result_list]
                result_list[index_of_last_element_in_result_list] = [["rowID", "INTEGER"]]
                for element in range(len(temp_list)):
                    result_list[index_of_last_element_in_result_list].append(temp_list[element])
                # Append table contents to the result list
                for row in table_contents:
                    result_list.append(row)
            else:
                # Table has a dedicated row ID column (integer primary key column), link values stored as row ID in the b-tree to this column (at the place of this column)
                # Append table contents to the result list
                for row in table_contents:
                    # Replace "None" entries in integer primary key column of each row through the actual row ID
                    row[ROW_ID_COLUMN] = row[0]
                    # Delete manually appended row ID column (in parse_sql_statement_params)
                    temp_row = row
                    row = []
                    for index in range(len(temp_row) - 1):
                        row.append(temp_row[index + 1])
                    # Append corrected row
                    result_list.append(row)

            # Append result from table, index, view or trigger to final list
            final_list.append(result_list)

            # TODO: comment out the following print statements in productive environment
            #_adel_log.log("\n_sqliteParser.py:234, parse_db ----> printing database schema for " + str(type_tuple[0]) + " \"" + str(name_tuple[0]) + "\" for test purposes:", 4)
            #_adel_log.log(str(db_schema[len(db_schema) - 1]), 4)
            #_adel_log.log("\n_sqliteParser.py:236, parse_db ----> printing database contents for " + str(type_tuple[0]) + " \"" + str(name_tuple[0]) + "\" for test purposes:", 4)
            #for result in result_list:
            #    _adel_log.log(str(result), 4)
            # comment out the above print statements in productive environment

        # PARSE INDEX STATEMENT
        #if ((str(db_schema[1]) == "INDEX") or (str(db_schema[1]) == "Index") or (str(db_schema[1]) == "index")):
        # TODO: implement if necessary
        # IGNORED RIGHT NOW

        # PARSE VIEW STATEMENT
        #if ((str(db_schema[1]) == "VIEW") or (str(db_schema[1]) == "View") or (str(db_schema[1]) == "view")):
        # TODO: implement if necessary
        # IGNORED RIGHT NOW

        # PARSE TRIGGER STATEMENT
        #if ((str(db_schema[1]) == "TRIGGER") or (str(db_schema[1]) == "Trigger") or (str(db_schema[1]) == "trigger")):
        # TODO: implement if necessary
        # IGNORED RIGHT NOW

    # Close the database file
    _sqliteFileHandler.close_db()

    return final_list