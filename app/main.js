// Refer to https://www.sqlite.org/fileformat.html

import { open, stat } from 'fs/promises';
import path from 'path';

const databaseFile = process.argv[2];
const command = process.argv[3];

function parsePageHeader(buffer, page, pageSize) {
  const offset = page === 0 ? 100 : 0;

  const pageType = buffer.readInt8(0 + offset);
  const startFreeBlock = buffer.readUInt16BE(1 + offset);
  const numberOfCells = buffer.readUInt16BE(3 + offset);
  const startCellContentArea = buffer.readUInt16BE(5 + offset);
  const numberOfFragmentedFreeBytes = buffer.readInt8(7 + offset);

  return { pageType, numberOfCells, startCellContentArea };
}

function convertToVarInt(value) {
  return (value - 13) / 2;
}

function parseTableSchema(buffer, numberOfCells, startCellContentArea, pageSize) {
  const cellContent = buffer.subarray(startCellContentArea, pageSize);

  const tableNames = [];

  let index = 0;

  for (let i = 0; i < numberOfCells; i++) {
    const recordSize = cellContent[index];
    const rowId = cellContent[index + 1];
    const recordHeaderSize = cellContent[index + 2];
    const schemaTypeSize = convertToVarInt(cellContent[index + 3]);
    const schemaSize = convertToVarInt(cellContent[index + 4]);
    const tableNameSize = convertToVarInt(cellContent[index + 5]);

    // calculate table name start position
    // first 2 bytes are allocated for the record size and row ID
    const tableNameStartPosition = index + 2 + recordHeaderSize + schemaTypeSize + schemaSize;

    const tableName = cellContent
      .subarray(tableNameStartPosition, tableNameStartPosition + tableNameSize)
      .toString('utf8');
    tableNames.push(tableName);

    // advance index to next record
    index += recordSize + 2;
  }

  return { tableNames };
}

async function parsePage(databaseFileHandler, page, pageSize) {
  // fetch page data
  const { buffer } = await databaseFileHandler.read({
    length: pageSize,
    position: page * pageSize,
    buffer: Buffer.alloc(pageSize),
  });

  const { pageType, numberOfCells, startCellContentArea } = parsePageHeader(buffer, page, pageSize);

  return { buffer, pageType, numberOfCells, startCellContentArea };
}

async function fetchTables(databaseFileHandler, pageSize) {
  const { buffer, pageType, numberOfCells, startCellContentArea } = await parsePage(databaseFileHandler, 0, pageSize);

  const { tableNames } = parseTableSchema(buffer, numberOfCells, startCellContentArea, pageSize);

  // size = 120, hexdump -C sample.db -s 3779 -n 122
  // size = 80, hexdump -C sample.db -s 3901 -n 82
  // size = 111, hexdump -C sample.db -s 3983 -n 113

  return { buffer, pageType, tableNames };
}

async function parseFileHeader(databaseFileHandler) {
  const { buffer: fileHeader } = await databaseFileHandler.read({
    length: 100,
    position: 0,
    buffer: Buffer.alloc(100),
  });

  const pageSize = fileHeader.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  const totalNumberOfPages = fileHeader.readUInt32BE(28); // total number of pages is 4 bytes starting at offset 28

  return {
    pageSize,
    totalNumberOfPages,
  };
}

function parseSqlCommand(command) {
  const parts = command.split(' ');
  const tableName = parts.pop();

  return { tableName };
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
    const { pageType, tableNames } = await fetchTables(databaseFileHandler, pageSize);

    if (command === '.dbinfo') {
      console.log(`database page size: ${pageSize}`);
      console.log(`number of tables: ${tableNames.length}`);
    } else if (command === '.tables') {
      // list table names but exclude the internal schema table
      const userTableNames = tableNames
        .filter((tableName) => tableName !== 'sqlite_sequence')
        .sort()
        .join(' ');
      console.log(userTableNames);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      let pageIndex = 1;

      const tables = [];

      for (const tableName of tableNames) {
        const { numberOfCells } = await parsePage(databaseFileHandler, pageIndex, pageSize);
        pageIndex++;
        tables.push({ tableName, rowCount: numberOfCells });
      }

      const { tableName: specifiedTableName } = parseSqlCommand(command);
      const result = tables.filter((table) => table.tableName === specifiedTableName);

      if (result.length > 0) {
        console.log(result[0].rowCount);
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
