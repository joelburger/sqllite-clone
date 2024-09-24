import { open, stat } from 'fs/promises';
import path from 'path';

const databaseFile = process.argv[2];
const command = process.argv[3];

async function fetchPageType(databaseFileHandler, page, pageSize) {
  const offset = page * pageSize;

  const { buffer } = await databaseFileHandler.read({
    length: pageSize,
    position: offset,
    buffer: Buffer.alloc(pageSize),
  });

  return buffer.readInt8(0);
}

if (command === '.dbinfo') {
  const filePath = path.join(process.cwd(), databaseFile);
  // console.log(`filePath ${filePath}`);
  const databaseFileHandler = await open(filePath, 'r');

  const { buffer: fileHeader } = await databaseFileHandler.read({
    length: 100,
    position: 0,
    buffer: Buffer.alloc(100),
  });

  const fileStats = await stat(filePath);
  const fileSize = fileStats.size;
  //console.log(`Total file size: ${fileSize} bytes`);

  const pageSize = fileHeader.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  console.log(`database page size: ${pageSize}`);

  const totalNumberOfPages = fileSize / pageSize;
  //console.log(`total number of pages: ${totalNumberOfPages}`);

  let tableCount = 0;
  for (let page = 1; page <= totalNumberOfPages; page++) {
    const pageType = await fetchPageType(databaseFileHandler, page, pageSize);
    if (pageType === 13 || pageType === 5) {
      tableCount++;
    }
    //console.log(`Page ${page} type: ${pageType}`);
  }
  console.log(`number of tables: ${tableCount}`);
} else {
  throw `Unknown command ${command}`;
}
