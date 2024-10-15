const { open } = require('fs/promises');
const path = require('path');
const { parseSelectCommand } = require('./sqlparser.js');
const readVarInt = require('./varint');
const { parseColumns, parseIndex } = require('./sqlparser');

const DATABASE_HEADER_SIZE = 100;
const DEBUG_MODE = process.env.DEBUG_MODE;
const TRACE_MODE = process.env.TRACE_MODE;

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
  const serialType = new Map();
  const { bytesRead } = readVarInt(buffer, 0);
  let cursor = bytesRead;
  for (const column of columns) {
    const { value, bytesRead } = readVarInt(buffer, cursor);
    cursor += bytesRead;
    serialType.set(column, value);
  }

  const record = new Map();
  for (const column of columns) {
    const { value, newCursor } = readValue(buffer, cursor, serialType.get(column));
    record.set(column, value);
    cursor = newCursor;
  }

  logTrace('parseRecord', { buffer, serialType, record });

  return record;
}

function parseTableOrIndexSchema(buffer) {
  const schemaColumns = ['schemaType', 'schemaName', 'name', 'rootPage', 'schemaBody'];
  return parseRecord(buffer, schemaColumns);
}

function readCell(pageType, buffer, cellPointer) {
  let cursor = cellPointer;
  const { value: recordSize, bytesRead } = readVarInt(buffer, cursor);
  cursor += bytesRead;

  let rowId;
  if (pageType === 0x0d || pageType === 0x05) {
    const { value, bytesRead: rowIdBytesRead } = readVarInt(buffer, cursor);
    rowId = value;
    cursor += rowIdBytesRead;
  }

  const startOfRecord = cursor;
  const endOfRecord = startOfRecord + recordSize;
  const record = buffer.subarray(startOfRecord, endOfRecord);

  logTrace('readCell', {
    pageType,
    cellPointer,
    recordSize,
    bytesRead,
    rowId,
    first10Bytes: record.subarray(0, 10),
    record: record.toString('utf8'),
  });

  return { record, rowId };
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
  if (DEBUG_MODE || TRACE_MODE) {
    console.log(...message);
  }
}

function logTrace(...message) {
  if (TRACE_MODE) {
    console.log(...message);
  }
}

function parseTableLeafPage(pageType, numberOfCells, buffer, columns, identityColumn) {
  let cursor = getPageHeaderSize(pageType);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { record, rowId } = readCell(pageType, buffer, cellPointer);
    const row = parseRecord(record, columns);
    if (identityColumn) {
      row.set(identityColumn, rowId);
    }
    rows.push(row);
    cursor += 2;
  }
  return rows;
}

function parseTableInteriorPage(page, pageType, numberOfCells, buffer) {
  let cursor = getPageHeaderSize(pageType);
  const childPointers = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const childPointer = buffer.readUInt32BE(cellPointer);
    childPointers.push(childPointer);
    cursor += 2;
  }
  logDebug('parseTableInteriorPage', { page, numberOfChildPointers: childPointers.length });
  return childPointers;
}

async function fetchPage(fileHandle, page, pageSize) {
  const offset = (page - 1) * pageSize;

  const { buffer } = await fileHandle.read({
    length: pageSize,
    position: offset,
    buffer: Buffer.alloc(pageSize),
  });

  return buffer;
}

function parsePageHeader(buffer, page, offset) {
  const pageType = buffer.readInt8(offset);
  const startOfFreeBlock = buffer.readUInt16BE(offset + 1);
  const numberOfCells = buffer.readUInt16BE(offset + 3);
  const startOfCellContentArea = buffer.readUInt16BE(offset + 5);
  const rightMostPointer = pageType === 0x02 || pageType === 0x05 ? buffer.readUInt32BE(offset + 8) : undefined;
  const pageHeaderSize = getPageHeaderSize(pageType);

  logTrace('parsePageHeader', {
    page,
    pageType,
    startOfFreeBlock,
    numberOfCells,
    startOfCellContentArea,
    rightMostPointer,
    pageHeaderSize,
  });

  return {
    pageType,
    startOfFreeBlock,
    numberOfCells,
    startOfCellContentArea,
    rightMostPointer,
    pageHeaderSize,
  };
}

async function readTableRows(fileHandle, page, pageSize, columns, identityColumn) {
  const buffer = await fetchPage(fileHandle, page, pageSize);
  const { pageType, numberOfCells } = parsePageHeader(buffer, page, 0);

  if (pageType === 0x0d) {
    return parseTableLeafPage(pageType, numberOfCells, buffer, columns, identityColumn);
  } else if (pageType === 0x05) {
    const rows = [];
    const childPointers = parseTableInteriorPage(page, pageType, numberOfCells, buffer);

    for (const childPointer of childPointers) {
      rows.push(...(await readTableRows(fileHandle, childPointer, pageSize, columns, identityColumn)));
    }
    return rows;
  }
  throw new Error(`Unknown page type: ${pageType}`);
}

async function readDatabaseSchemas(fileHandle, pageSize) {
  const buffer = await fetchPage(fileHandle, 0, pageSize);
  const { pageType, numberOfCells, pageHeaderSize } = parsePageHeader(buffer, 1, DATABASE_HEADER_SIZE);

  let cursor = pageHeaderSize + DATABASE_HEADER_SIZE;
  const tables = [];
  const indexes = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { record } = readCell(pageType, buffer, cellPointer);
    const databaseObject = parseTableOrIndexSchema(record);
    const schemaType = databaseObject.get('schemaType');
    if (schemaType === 'table') {
      tables.push(databaseObject);
    } else if (schemaType === 'index') {
      indexes.push(databaseObject);
    } else {
      throw new Error(`Invalid schema type: ${schemaType}`);
    }
    cursor += 2;
  }

  return { tables, indexes };
}

function projectTableRows(rows, queryColumns) {
  return rows.map((row) => queryColumns.map((queryColumn) => row.get(queryColumn)).join('|'));
}

function filterAndFormatListOfTables(tables) {
  return tables
    .map((table) => table.get('name'))
    .filter((tableName) => tableName !== 'sqlite_sequence')
    .sort()
    .join(' ');
}

function searchIndex(queryTableName, indexes) {
  // TODO add columns to the filter
  return indexes.find((index) => parseIndex(index.get('schemaBody')).tableName === queryTableName);
}

async function handleSelectCommand(command, fileHandle, pageSize, tables, indexes) {
  const { queryColumns, queryTableName, whereClause } = parseSelectCommand(command);
  const table = tables.find((table) => table.get('name') === queryTableName);
  if (!table) {
    throw new Error(`Table ${queryTableName} not found`);
  }
  const { columns, identityColumn } = parseColumns(table.get('schemaBody'));
  const index = searchIndex(queryTableName, indexes);
  logDebug('handleSelectCommand', {
    index,
  });

  const rows = await readTableRows(fileHandle, table.get('rootPage'), pageSize, columns, identityColumn);
  const filteredRows = applyFilter(rows, whereClause);

  if (queryColumns[0] === 'count(*)') {
    console.log(filteredRows.length);
  } else {
    const result = projectTableRows(filteredRows, queryColumns);
    console.log(result.join('\n'));
  }
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
    const { pageSize } = await readDatabaseHeader(fileHandle);
    const { tables, indexes } = await readDatabaseSchemas(fileHandle, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tables.length}`);
    } else if (command === '.tables') {
      const userTables = filterAndFormatListOfTables(tables);
      console.log(userTables);
    } else if (command === '.indexes') {
      console.log(indexes.map((index) => index.get('name')).join(' '));
    } else if (command.toUpperCase().startsWith('SELECT')) {
      await handleSelectCommand(command, fileHandle, pageSize, tables, indexes);
    }
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    if (fileHandle) {
      await fileHandle.close();
    }
  }
}

main();
