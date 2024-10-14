const { open } = require('fs/promises');
const path = require('path');
const { parseSelectCommand } = require('./sqlparser.js');
const readVarInt = require('./varint');

const DATABASE_HEADER_SIZE = 100;
const DEBUG_MODE = process.env.DEBUG_MODE;

function getPageHeaderSize(pageType) {
  if (pageType === 13 || pageType === 10) {
    return 8;
  } else if (pageType === 2 || pageType === 5) {
    return 12;
  }
  throw new Error(`invalid page type: ${pageType}`);
}

async function readDatabaseHeader(fileHandle) {
  const { buffer } = await fileHandle.read({
    length: DATABASE_HEADER_SIZE,
    position: 0,
    buffer: Buffer.alloc(DATABASE_HEADER_SIZE),
  });

  const pageSize = buffer.readUInt16BE(16);
  const numberOfPages = buffer.readUInt32BE(28);

  return { pageSize, numberOfPages };
}

function decodeSerialTypeCode(value) {
  if (value < 12) {
    return value;
  } else if (value % 2 === 0) {
    return (value - 12) / 2;
  }
  return (value - 13) / 2;
}

function parseColumns(tableSchema) {
  const pattern = /^CREATE\s+TABLE\s+[\w\"]+\s*\(\s*(?<columns>[\s\S_]+)\s*\)$/i;
  const columns = pattern.exec(tableSchema)?.groups.columns || '';

  if (!columns) {
    throw new Error(`Failed to parse columns from "${tableSchema}".`);
  }

  return columns.split(',').map((value) => value.trim().split(' ')[0]);
}

function readValue(buffer, cursor, serialType) {
  if ([0, 8, 9, 12, 13].includes(serialType)) return { value: null, newCursor: cursor + 1 };
  if (serialType > 12) {
    const dataTypeSize = (serialType - (serialType % 2 === 0 ? 12 : 13)) / 2;
    const value = buffer.subarray(cursor, cursor + dataTypeSize);
    const newCursor = cursor + dataTypeSize;
    if (serialType % 2 === 0) {
      return { value, newCursor };
    }
    return { value: value.toString('utf8'), newCursor };
  }
  if (serialType === 1) return { value: buffer.readInt8(cursor), newCursor: cursor + 1 };
  if (serialType === 2) return { value: buffer.readUInt16BE(cursor), newCursor: cursor + 2 };
  if (serialType === 3) return { value: buffer.readUIntBE(cursor, 3), newCursor: cursor + 3 };
  if (serialType === 4) return { value: buffer.readUInt32BE(cursor), newCursor: cursor + 4 };
  if (serialType === 5) return { value: buffer.readUInt48BE(cursor), newCursor: cursor + 6 };
  if ([6, 7].includes(serialType)) return { value: buffer.readBigUInt64BE(cursor), newCursor: cursor + 8 };
  throw new Error(`Unknown serial type: ${serialType}`);
}

function parseRow(buffer, columns) {
  const row = new Map();
  const columnDataType = new Map();
  let cursor = 0;
  for (const column of columns) {
    cursor++;
    columnDataType.set(column, buffer.readInt8(cursor));
  }
  for (const column of columns) {
    const { value, newCursor } = readValue(buffer, cursor, columnDataType.get(column));
    row.set(column, value);
    cursor = newCursor;
  }

  return row;
}

function parseTableSchema(buffer) {
  const { value: headerSize } = readVarInt(buffer, 0);
  const schemaTypeSize = decodeSerialTypeCode(buffer[1]);
  const schemaNameSize = decodeSerialTypeCode(buffer[2]);
  const tableNameSize = decodeSerialTypeCode(buffer[3]);
  const rootPageSize = decodeSerialTypeCode(buffer[4]);

  logDebug('parseTableSchema', {
    headerSize,
    schemaTypeSize,
    schemaNameSize,
    tableNameSize,
    rootPageSize,
  });

  let cursor = headerSize;
  const schemaType = buffer.subarray(cursor, cursor + schemaTypeSize).toString('utf8');
  cursor += schemaTypeSize;
  const schemaName = buffer.subarray(cursor, cursor + schemaNameSize).toString('utf8');
  cursor += schemaNameSize;
  const tableName = buffer.subarray(cursor, cursor + tableNameSize).toString('utf8');
  cursor += tableNameSize;
  const rootPage = decodeSerialTypeCode(buffer[cursor]);
  cursor++;
  const schemaBody = buffer.subarray(cursor).toString('utf8');

  logDebug({ schemaType, schemaName, tableName, rootPage, schemaBody });

  const columns = parseColumns(schemaBody);

  return {
    tableName,
    columns,
    rootPage,
  };
}

function readCell(pageType, buffer, cellPointer) {
  let cursor = cellPointer;
  const { value: recordSize, bytesRead } = readVarInt(buffer, cursor);

  logDebug('readCell', {
    first10Bytes: buffer.subarray(cursor, cursor + 10),
    pageType,
    cursor,
    recordSize,
    bytesRead,
  });

  cursor += bytesRead;

  if (pageType === 0x0d || pageType === 0x05) {
    cursor++; // skip rowId
  }
  const startOfRecord = cursor;
  const endOfRecord = startOfRecord + recordSize;
  return buffer.subarray(startOfRecord, endOfRecord);
}

function applyFilter(rows, whereClause) {
  if (whereClause.length === 0) {
    return rows;
  }
  const [filterColumn, filterValue] = whereClause[0];
  return rows.filter((row) => {
    return row.get(filterColumn) === filterValue;
  });
}

function logDebug(...message) {
  if (DEBUG_MODE) {
    console.log(...message);
  }
}

async function readTableContents(fileHandle, table, pageSize, whereClause) {
  const offset = (table.rootPage - 1) * pageSize;

  const { buffer } = await fileHandle.read({
    length: pageSize,
    position: offset,
    buffer: Buffer.alloc(pageSize),
  });

  const pageType = buffer.readInt8(0);
  const startOfFreeBlock = buffer.readUInt16BE(1);
  const numberOfCells = buffer.readUInt16BE(3);
  const startOfCellContentArea = buffer.readUInt16BE(5);
  const rightMostPointer = pageType === 0x02 || pageType === 0x05 ? buffer.readUInt32BE(8) : undefined;

  let cursor = getPageHeaderSize(pageType);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const record = readCell(pageType, buffer, cellPointer);
    rows.push(parseRow(record, table.columns));
    cursor += 2;
  }
  return applyFilter(rows, whereClause);
}

async function readDatabaseSchemas(fileHandle, pageSize) {
  const { buffer } = await fileHandle.read({
    length: pageSize,
    position: 0,
    buffer: Buffer.alloc(pageSize),
  });

  const offset = DATABASE_HEADER_SIZE; //   skip the first 100 bytes allocated to the database header
  const pageType = buffer.readInt8(offset);
  const numberOfCells = buffer.readUInt16BE(3 + offset);
  const pageHeaderSize = getPageHeaderSize(pageType);

  let cursor = pageHeaderSize + offset;

  const tables = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const record = readCell(pageType, buffer, cellPointer);
    const table = parseTableSchema(record);
    tables.push(table);
    cursor += 2;
  }

  return tables;
}

function formatTableContents(tableContents, queryColumns) {
  return tableContents.map((row) => queryColumns.map((queryColumn) => row.get(queryColumn)).join('|'));
}

function formatListOfTables(tables) {
  return tables
    .map((tableSchema) => tableSchema.tableName)
    .filter((tableName) => tableName !== 'sqlite_sequence')
    .sort()
    .join(' ');
}

async function main() {
  if (DEBUG_MODE) {
    console.log('Debug mode enabled');
  }

  const databaseFile = process.argv[2];
  const command = process.argv[3];

  let fileHandle;
  try {
    const filePath = path.join(process.cwd(), databaseFile);
    fileHandle = await open(filePath, 'r');
    const { pageSize, numberOfPages } = await readDatabaseHeader(fileHandle);
    const tables = await readDatabaseSchemas(fileHandle, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tables.length}`);
    } else if (command === '.tables') {
      const userTables = formatListOfTables(tables);
      console.log(userTables);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      const { queryColumns, queryTableName, whereClause } = parseSelectCommand(command);
      const table = tables.find((table) => table.tableName === queryTableName);
      if (!table) {
        throw new Error(`Table ${queryTableName} not found`);
      }
      const tableContents = await readTableContents(fileHandle, table, pageSize, whereClause);

      if (queryColumns[0] === 'count(*)') {
        console.log(tableContents.length);
      } else {
        const result = formatTableContents(tableContents, queryColumns);
        console.log(result.join('\n'));
      }
    }
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    if (fileHandle) {
      fileHandle.close();
    }
  }
}

main();
