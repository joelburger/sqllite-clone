import { open } from 'fs/promises';
import path from 'path';

const DATABASE_HEADER_SIZE = 100;

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

  return { pageSize };
}

function readVarInt(value) {
  if (value < 12) {
    return value;
  } else if (value % 2 === 0) {
    return (value - 12) / 2;
  }
  return (value - 13) / 2;
}

function parseColumns(tableSchema) {
  const pattern = /^CREATE\s+TABLE\s+\w+\s*\(\s*(?<columns>[\s\S]+)\s*\)$/i;
  const columns = pattern.exec(tableSchema)?.groups.columns || '';

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
  if (serialType === 3) return { value: buffer.readUInt24BE(cursor), newCursor: cursor + 3 };
  if (serialType === 4) return { value: buffer.readUInt32BE(cursor), newCursor: cursor + 4 };
  if (serialType === 5) return { value: buffer.readUInt48BE(cursor), newCursor: cursor + 6 };
  if ([6, 7].includes(serialType)) return { value: buffer.readUInt64BE(cursor), newCursor: cursor + 8 };
  throw new Error(`Unknown serial type: ${serialType}`);
}

function parseRow(buffer, columns) {
  const headerSize = readVarInt(buffer.readInt8(0));
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
  const headerSize = readVarInt(buffer.readInt8(0));
  const schemaTypeSize = readVarInt(buffer.readInt8(1));
  const schemaNameSize = readVarInt(buffer.readInt8(2));
  const tableNameSize = readVarInt(buffer.readInt8(3));
  const rootPageSize = readVarInt(buffer[4]);
  const schemaBodySize = headerSize === 7 ? readVarInt(buffer[5] + buffer[6]) : readVarInt(buffer[5]);

  let cursor = headerSize;
  const schemaType = buffer.subarray(cursor, cursor + schemaTypeSize).toString('utf8');
  cursor += schemaTypeSize;
  const schemaName = buffer.subarray(cursor, cursor + schemaNameSize).toString('utf8');
  cursor += schemaNameSize;
  const tableName = buffer.subarray(cursor, cursor + tableNameSize).toString('utf8');
  cursor += tableNameSize;
  const rootPage = readVarInt(buffer.readInt8(cursor));
  cursor++;
  const schemaBody = buffer.subarray(cursor, cursor + schemaBodySize);
  const columns = parseColumns(schemaBody);

  return {
    tableName,
    columns,
    rootPage,
  };
}

function readCell(buffer, cellPointer) {
  let cursor = cellPointer;
  const recordSize = buffer.readInt8(cursor);
  const rowId = readVarInt(buffer.readInt8(cursor + 1));
  return buffer.subarray(cursor + 2, cursor + 2 + recordSize);
}

async function readTableContents(fileHandle, rootPage, columns, pageSize) {
  const offset = (rootPage - 1) * pageSize;

  const { buffer } = await fileHandle.read({
    length: pageSize,
    position: offset,
    buffer: Buffer.alloc(pageSize),
  });

  const pageType = buffer.readInt8(0);
  const numberOfCells = buffer.readUInt16BE(3);
  let cursor = getPageHeaderSize(pageType);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const record = readCell(buffer, cellPointer);
    rows.push(parseRow(record, columns));
    cursor += 2;
  }

  return rows;
}

async function readDatabaseSchemas(fileHandle, pageSize) {
  const { buffer } = await fileHandle.read({
    length: pageSize,
    position: 0,
    buffer: Buffer.alloc(pageSize),
  });

  const offset = DATABASE_HEADER_SIZE; //   skip the first 100 bytes allocated to the database header

  const pageType = buffer.readInt8(0 + offset);
  const numberOfCells = buffer.readUInt16BE(3 + offset);
  const pageHeaderSize = getPageHeaderSize(pageType);

  let cursor = pageHeaderSize + offset;

  const tables = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const record = readCell(buffer, cellPointer);

    const table = parseTableSchema(record);
    tables.push(table);
    cursor += 2;
  }

  return tables;
}

function parseSelectCommand(command) {
  const pattern = /select\s+(?<columns>[\w\(\)\*]+)\s+from\s+(?<tableName>[\w]+)/i;

  const queryColumns = pattern.exec(command)?.groups.columns;
  const queryTableName = pattern.exec(command)?.groups.tableName;

  return { queryTableName, queryColumns };
}

async function main() {
  const databaseFile = process.argv[2];
  const command = process.argv[3];

  let fileHandle;
  try {
    const filePath = path.join(process.cwd(), databaseFile);
    fileHandle = await open(filePath, 'r');

    const { pageSize } = await readDatabaseHeader(fileHandle);

    const tables = await readDatabaseSchemas(fileHandle, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tables.length}`);
    } else if (command === '.tables') {
      const userTables = tables
        .map((tableSchema) => tableSchema.tableName)
        .filter((tableName) => tableName !== 'sqlite_sequence')
        .sort()
        .join(' ');
      console.log(userTables);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      const { queryColumns, queryTableName } = parseSelectCommand(command);
      const table = tables.find((table) => table.tableName === queryTableName);
      if (!table) {
        throw new Error(`Table ${queryTableName} not found`);
      }
      const tableContents = await readTableContents(fileHandle, table.rootPage, table.columns, pageSize);

      if (queryColumns === 'count(*)') {
        console.log(tableContents.length);
      } else {
        const result = tableContents.map((row) => row.get(queryColumns));
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

await main();
