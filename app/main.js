import { open, stat } from 'fs/promises';
import path from 'path';

const databaseFile = process.argv[2];
const command = process.argv[3];

async function parsePageHeader(databaseFileHandler, page, pageSize) {
  const offset = page === 0 ? 100 : 0;

  const { buffer } = await databaseFileHandler.read({
    length: pageSize,
    position: page * pageSize,
    buffer: Buffer.alloc(pageSize),
  });

  const pageType = buffer.readInt8(0 + offset);
  const startFreeBlock = buffer.readUInt16BE(1 + offset);
  const numberOfCells = buffer.readUInt16BE(3 + offset);
  const startCellContentArea = buffer.readUInt16BE(5 + offset);
  const numberOfFragmentedFreeBytes = buffer.readInt8(7 + offset);

  console.log('page', page);
  console.log('pageType', pageType);
  console.log('startFreeBlock', startFreeBlock);
  console.log('numberOfCells', numberOfCells);
  console.log('startCellContentArea', startCellContentArea);
  console.log('numberOfFragmentedFreeBytes', numberOfFragmentedFreeBytes);

  const start = startCellContentArea;
  const end = pageSize;

  const cellContent = buffer.subarray(start, end);

  //console.log(`cellContent ${start}-${end}`, cellContent.toString('utf8'));

  console.log('total size', cellContent.length);

  let index = 0;
  let recordCount = 0;

  while (recordCount < numberOfCells) {
    // read record size
    const recordSize = cellContent[index];
    const rowId = cellContent[index + 1];
    const recordHeaderSize = cellContent[index + 2];
    const schemaTypeSize = cellContent[index + 3];
    const schemaSize = cellContent[index + 4];
    const tableNameSize = cellContent[index + 5];

    console.log('>>>>>>>>>>>>> recordSize', recordSize);
    console.log('>>>>>>>>>>>>> rowId', rowId);
    console.log('>>>>>>>>>>>>> recordHeaderSize', recordHeaderSize);
    console.log('>>>>>>>>>>>>> schemaTypeSize', (schemaTypeSize - 13) / 2);
    console.log('>>>>>>>>>>>>> schemaSize', (schemaSize - 13) / 2);
    console.log('>>>>>>>>>>>>> tableNameSize', (tableNameSize - 13) / 2);

    // print record
    console.log('>>>>>>>>>>>>> record', cellContent.subarray(index, index + recordSize + 2).toString('utf8'));

    // advance index to next record
    index += recordSize + 2;

    // increment record counter
    recordCount++;
  }

  // size = 120, hexdump -C sample.db -s 3779 -n 122
  // size = 80, hexdump -C sample.db -s 3901 -n 82
  // size = 111, hexdump -C sample.db -s 3983 -n 113

  console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

  return { pageType, startCellContentArea, numberOfCells };
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

if (command === '.dbinfo') {
  // Refer to https://www.sqlite.org/fileformat.html

  // read database file
  const filePath = path.join(process.cwd(), databaseFile);
  const databaseFileHandler = await open(filePath, 'r');

  // parse file header
  const { pageSize, totalNumberOfPages } = await parseFileHeader(databaseFileHandler);
  console.log(`database page size: ${pageSize}`);

  let tableCount = 0;
  for (let page = 1; page < totalNumberOfPages; page++) {
    const { pageType } = await parsePageHeader(databaseFileHandler, page, pageSize);
    if (pageType === 13 || pageType === 5) {
      tableCount++;
    }
  }
  console.log(`number of tables: ${tableCount}`);
} else if (command === '.tables') {
} else {
  throw `Unknown command ${command}`;
}
