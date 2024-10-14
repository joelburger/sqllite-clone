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

  logDebug('readDatabaseHeader', { pageSize, numberOfPages });

  return { pageSize, numberOfPages };
}

function calculateSerialTypeContentSize({ value }) {
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
  if ([0, 8, 9, 12, 13].includes(serialType)) return { value: null, newCursor: cursor };
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
  if (serialType === 5) return { value: buffer.readUIntBE(cursor, 6), newCursor: cursor + 6 };
  if ([6, 7].includes(serialType)) return { value: buffer.readBigUInt64BE(cursor), newCursor: cursor + 8 };
  throw new Error(`Unknown serial type: ${serialType}`);
}

function parseRecord(buffer, columns) {
  const columnDataType = new Map();
  const { value: headerSize, bytesRead } = readVarInt(buffer, 0);
  let cursor = bytesRead;
  for (const column of columns) {
    const { value, bytesRead } = readVarInt(buffer, cursor);
    cursor += bytesRead;
    columnDataType.set(column, value);
  }

  const record = new Map();
  for (const column of columns) {
    const { value, newCursor } = readValue(buffer, cursor, columnDataType.get(column));
    record.set(column, value);
    cursor = newCursor;
  }

  return record;
}

function parseTableSchema(buffer) {
  const { value: headerSize } = readVarInt(buffer, 0);
  const schemaColumns = ['schemaType', 'schemaName', 'tableName', 'rootPage', 'schemaBody'];
  return parseRecord(buffer, schemaColumns);
}

function readCell(pageType, buffer, cellPointer) {
  let cursor = cellPointer;
  const { value: recordSize, bytesRead } = readVarInt(buffer, cursor);
  cursor += bytesRead;

  if (pageType === 0x0d || pageType === 0x05) {
    cursor++; // skip rowId
  }

  const startOfRecord = cursor;
  const endOfRecord = startOfRecord + recordSize;
  const record = buffer.subarray(startOfRecord, endOfRecord);

  logDebug('readCell', {
    first10Bytes: buffer.subarray(cursor, cursor + 10),
    pageType,
    cellPointer,
    recordSize,
    bytesRead,
    record: record.toString('utf8'),
  });

  return record;
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

function parseTableLeafPage(pageType, numberOfCells, buffer, columns) {
  let cursor = getPageHeaderSize(pageType);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const record = readCell(pageType, buffer, cellPointer);
    const row = parseRecord(record, columns);
    if (row.has('name') && row.get('name')) {
      rows.push(row);
    }
    cursor += 2;
  }
  return rows;
}

function parseTableInteriorPage(pageType, numberOfCells, buffer) {
  let cursor = getPageHeaderSize(pageType);
  const childPointers = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const childPointer = buffer.readUInt32BE(cellPointer);
    childPointers.push(childPointer);
    cursor += 2;
  }
  logDebug('parseTableInteriorPage', { childPointers });
  return childPointers;
}

async function readTableRows(fileHandle, page, pageSize, columns) {
  const offset = (page - 1) * pageSize;

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

  if (pageType === 0x0d) {
    return parseTableLeafPage(pageType, numberOfCells, buffer, columns);
  } else if (pageType === 0x05) {
    const rows = [];
    const childPointers = parseTableInteriorPage(pageType, numberOfCells, buffer);

    for (const childPointer of childPointers) {
      rows.push(...(await readTableRows(fileHandle, childPointer, pageSize, columns)));
    }
    return rows;
  }
  throw new Error(`Unknown page type: ${pageType}`);
}

async function readTableSchemas(fileHandle, pageSize) {
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

function projectTableRows(rows, queryColumns) {
  return rows.map((row) => queryColumns.map((queryColumn) => row.get(queryColumn)).join('|'));
}

function formatListOfTables(tables) {
  return tables
    .map((table) => table.get('tableName'))
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
    const tables = await readTableSchemas(fileHandle, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tables.length}`);
    } else if (command === '.tables') {
      const userTables = formatListOfTables(tables);
      console.log(userTables);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      const { queryColumns, queryTableName, whereClause } = parseSelectCommand(command);
      const table = tables.find((table) => table.get('tableName') === queryTableName);
      if (!table) {
        throw new Error(`Table ${queryTableName} not found`);
      }
      const columns = parseColumns(table.get('schemaBody'));
      const rows = await readTableRows(fileHandle, table.get('rootPage'), pageSize, columns);
      const filteredRows = applyFilter(rows, whereClause);

      if (queryColumns[0] === 'count(*)') {
        console.log(filteredRows.length);
      } else {
        const result = projectTableRows(filteredRows, queryColumns);
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
