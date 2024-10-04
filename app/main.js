// Refer to https://www.sqlite.org/fileformat.html

import { open, stat } from 'fs/promises';
import path from 'path';

const databaseFile = process.argv[2];
const command = process.argv[3];

const FILE_HEADER_SIZE = 100;

function parsePageHeader(buffer, page, pageSize) {
  const offset = page === 0 ? FILE_HEADER_SIZE : 0;

  const pageType = buffer.readInt8(0 + offset);
  const startFreeBlock = buffer.readUInt16BE(1 + offset);
  const numberOfCells = buffer.readUInt16BE(3 + offset);
  const startCellContentArea = buffer.readUInt16BE(5 + offset);
  const numberOfFragmentedFreeBytes = buffer.readInt8(7 + offset);

  return { pageType, numberOfCells, startCellContentArea };
}

function convertVarInt(value) {
  if (value < 12) {
    return value;
  } else if (value % 2 === 0) {
    return (value - 12) / 2;
  }
  return (value - 13) / 2;
}

function parseColumns(tableSchema) {
  const pattern = /^CREATE\s+TABLE\s+\w+\s+\(\s+(?<columns>[\s\S]+)\)$/i;
  const rawColumns = pattern.exec(tableSchema)?.groups.columns || '';

  return rawColumns.split(',').map((value) => value.trim().split(' ')[0]);
}

function parseTableSchemas(buffer, numberOfCells, startCellContentArea, pageSize) {
  const cellContent = buffer.subarray(startCellContentArea, pageSize);
  const tableSchemas = [];
  let index = 0;
  for (let i = 0; i < numberOfCells; i++) {
    const recordSize = cellContent[index];
    const rowId = cellContent[index + 1];
    const recordHeaderSize = cellContent[index + 2];
    const schemaTypeSize = convertVarInt(cellContent[index + 3]);
    const schemaSize = convertVarInt(cellContent[index + 4]);
    const tableNameSize = convertVarInt(cellContent[index + 5]);

    // calculate table name start position
    // first 2 bytes are allocated for the record size and row ID
    const tableNameStartPosition = index + 2 + recordHeaderSize + schemaTypeSize + schemaSize;
    const tableNameEndPosition = tableNameStartPosition + tableNameSize;

    const tableName = cellContent.subarray(tableNameStartPosition, tableNameEndPosition).toString('utf8');
    const tableSchema = cellContent.subarray(tableNameEndPosition + 1, recordSize + 2).toString('utf8');
    const columns = parseColumns(tableSchema);
    tableSchemas.push({ tableName, columns });

    // advance index to next record
    index += recordSize + 2;
  }

  return tableSchemas;
}

async function fetchPageData(databaseFileHandler, page, pageSize) {
  const { buffer } = await databaseFileHandler.read({
    length: pageSize,
    position: page * pageSize,
    buffer: Buffer.alloc(pageSize),
  });

  return buffer;
}

async function parsePage(databaseFileHandler, page, pageSize) {
  const buffer = await fetchPageData(databaseFileHandler, page, pageSize);
  const { pageType, numberOfCells, startCellContentArea } = parsePageHeader(buffer, page, pageSize);

  return { buffer, pageType, numberOfCells, startCellContentArea };
}

async function fetchTables(databaseFileHandler, pageSize) {
  const { buffer, pageType, numberOfCells, startCellContentArea } = await parsePage(databaseFileHandler, 0, pageSize);
  const tableSchemas = parseTableSchemas(buffer, numberOfCells, startCellContentArea, pageSize);

  return { buffer, pageType, tableSchemas };
}

async function parseFileHeader(databaseFileHandler) {
  const buffer = await fetchPageData(databaseFileHandler, 0, FILE_HEADER_SIZE);
  const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  const totalNumberOfPages = buffer.readUInt32BE(28); // total number of pages is 4 bytes starting at offset 28

  return {
    pageSize,
    totalNumberOfPages,
  };
}

function parseSelectCommand(command) {
  const pattern = /select\s+(?<columns>[\w\(\)\*]+)\s+from\s+(?<tableName>[\w]+)/i;

  const queryColumns = pattern.exec(command)?.groups.columns;
  const queryTableName = pattern.exec(command)?.groups.tableName;

  return { queryTableName, queryColumns };
}

function parseRecord(columnSizes, recordBody) {
  const columns = [];
  let cursor = 0;
  for (const columnSize of columnSizes) {
    const column = recordBody.subarray(cursor, cursor + columnSize)?.toString('utf8');
    columns.push(column);
    cursor += columnSize;
  }

  return columns;
}

async function readCellContents(databaseFileHandler, page, pageSize) {
  const buffer = await fetchPageData(databaseFileHandler, page, pageSize);
  const { pageType, numberOfCells, startCellContentArea } = parsePageHeader(buffer, page, pageSize);

  // A value of 2 (0x02) means the page is an interior cursor b-tree page.
  // A value of 5 (0x05) means the page is an interior table b-tree page.
  // A value of 10 (0x0a) means the page is a leaf cursor b-tree page.
  // A value of 13 (0x0d) means the page is a leaf table b-tree page.
  // console.log('pageType', pageType);

  const cellContent = buffer.subarray(startCellContentArea, pageSize);

  // skip two bytes at the start of each cell
  let cursor = 2;

  while (cursor < pageSize) {
    const headerSize = convertVarInt(cellContent[cursor]);
    cursor++;

    const columnSizes = cellContent.subarray(cursor, cursor + (headerSize - 1)).map((value) => convertVarInt(value));
    cursor += headerSize - 1;

    const recordBodySize = columnSizes.reduce((acc, columnSize) => (acc += columnSize), 0);
    const recordBody = cellContent.subarray(cursor, cursor + recordBodySize);
    const record = parseRecord(columnSizes, recordBody);

    cursor += recordBodySize;

    // skip two bytes at the start of each cell
    cursor += 2;
  }
}

async function main() {
  let databaseFileHandler;
  try {
    // read database file
    const filePath = path.join(process.cwd(), databaseFile);
    databaseFileHandler = await open(filePath, 'r');

    // parse file header
    const { pageSize, totalNumberOfPages } = await parseFileHeader(databaseFileHandler);

    // retrieve table information from the first page
    const { pageType, tableSchemas } = await fetchTables(databaseFileHandler, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tableSchemas.length}`);
    } else if (command === '.tables') {
      // list table names but exclude the internal schema table
      const userTableNames = tableSchemas
        .map((tableSchema) => tableSchema.tableName)
        .filter((tableName) => tableName !== 'sqlite_sequence')
        .sort()
        .join(' ');
      console.log(userTableNames);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      const tables = [];

      let pageIndex = 1;
      for (const tableSchema of tableSchemas) {
        const { numberOfCells } = await parsePage(databaseFileHandler, pageIndex, pageSize);
        const rows = await readCellContents(databaseFileHandler, pageIndex, pageSize);
        tables.push({ tableName: tableSchema.tableName, rowCount: numberOfCells, rows });
        pageIndex++;
      }

      const { queryColumns, queryTableName } = parseSelectCommand(command);
      const result = tables.filter((table) => table.tableName === queryTableName);

      if (queryColumns === 'count(*)') {
        if (result.length > 0) {
          console.log(result[0].rowCount);
        }
      } else {
      }
    } else {
      console.error(`Unknown command ${command}`);
    }
  } catch (err) {
    console.error('Fatal error', err);
  } finally {
    if (databaseFileHandler) {
      await databaseFileHandler.close();
    }
  }
}

await main();
