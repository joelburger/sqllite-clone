const { logDebug, logTrace } = require('./logger');
const readVarInt = require('./varint');

const DATABASE_HEADER_SIZE = 100;

function getPageHeaderSize(pageType) {
  if (pageType === 0x0a || pageType === 0x0d) {
    return 8;
  } else if (pageType === 0x02 || pageType === 0x05) {
    return 12;
  }
  throw new Error(`invalid page type: ${pageType}`);
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

function parseDatabaseSchemas(buffer) {
  const schemaColumns = ['schemaType', 'schemaName', 'name', 'rootPage', 'schemaBody'];
  return parseTableRow(buffer, schemaColumns);
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

function parseTableLeafPage(pageType, numberOfCells, buffer, columns, identityColumn, indexData) {
  let cursor = getPageHeaderSize(pageType);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { record, rowId } = readCell(pageType, buffer, cellPointer);
    const row = parseTableRow(record, columns);
    if (identityColumn) {
      row.set(identityColumn, rowId);
    }
    if (indexData) {
      const found = indexData.find((entry) => entry[1] === rowId);
      if (found) rows.push(row);
    } else {
      rows.push(row);
    }

    cursor += 2;
  }
  return rows;
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

async function readIndexPage(fileHandle, page, pageSize, filterValue) {
  const buffer = await fetchPage(fileHandle, page, pageSize);
  const { pageType, numberOfCells, rightMostPointer } = parsePageHeader(buffer, page, 0);
  const results = [];
  if (pageType === 0x02) {
    const keys = parseIndexInteriorPage(page, pageType, numberOfCells, buffer);
    for (const key of keys) {
      if (key.value >= filterValue) {
        const subresult = await readIndexPage(fileHandle, key.page, pageSize, filterValue);
        if (subresult.length === 0) {
          break;
        }
        results.push(...subresult);
      }
    }
    if (rightMostPointer !== undefined) {
      const subresult = await readIndexPage(fileHandle, rightMostPointer, pageSize, filterValue);
      results.push(...subresult);
    }
  } else if (pageType === 0x0a) {
    const indexData = parseIndexLeafPage(fileHandle, page, pageSize, pageType, numberOfCells, buffer, filterValue);
    results.push(...indexData);
  }

  return results;
}

async function readDatabaseSchemas(fileHandle, pageSize) {
  const buffer = await fetchPage(fileHandle, 1, pageSize);
  const { pageType, numberOfCells, pageHeaderSize } = parsePageHeader(buffer, 1, DATABASE_HEADER_SIZE);

  let cursor = pageHeaderSize + DATABASE_HEADER_SIZE;
  const tables = [];
  const indexes = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { record } = readCell(pageType, buffer, cellPointer);
    const databaseObject = parseDatabaseSchemas(record);
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

function parseTableRow(buffer, columns) {
  const serialType = new Map();
  const { bytesRead } = readVarInt(buffer, 0);
  let cursor = bytesRead;
  for (const column of columns) {
    const { value, bytesRead } = readVarInt(buffer, cursor);
    cursor += bytesRead;
    serialType.set(column, value);
  }

  const row = new Map();
  for (const column of columns) {
    const { value, newCursor } = readValue(buffer, cursor, serialType.get(column));
    row.set(column, value);
    cursor = newCursor;
  }

  logTrace('parseTableRow', { buffer, serialType, row });

  return row;
}

function parseIndexRow(buffer) {
  let cursor = 0;
  const { value: headerSize, bytesRead } = readVarInt(buffer, cursor);
  cursor += bytesRead;
  const serialTypes = [];
  while (cursor < headerSize) {
    const { value: serialType, bytesRead: serialTypeBytesRead } = readVarInt(buffer, cursor);
    serialTypes.push(serialType);
    cursor += serialTypeBytesRead;
  }
  return serialTypes.map((serialType) => {
    const { value, newCursor } = readValue(buffer, cursor, serialType);
    cursor = newCursor;
    return value;
  });
}

function parseTableInteriorPage(page, pageType, numberOfCells, buffer) {
  let cursor = getPageHeaderSize(pageType);
  const childPointers = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const page = buffer.readUInt32BE(cellPointer);
    const { value: rowId } = readVarInt(buffer, cellPointer + 4);
    childPointers.push({ page, rowId });
    cursor += 2;
  }
  return childPointers;
}

function parseIndexLeafPage(fileHandle, page, pageSize, pageType, numberOfCells, buffer, filterValue) {
  let cursor = getPageHeaderSize(pageType);

  const entries = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    cursor += 2;
    let cellCursor = cellPointer;
    const { value: payloadSize, bytesRead } = readVarInt(buffer, cellCursor);
    cellCursor += bytesRead;
    const payload = buffer.subarray(cellCursor, cellCursor + payloadSize);
    const entry = parseIndexRow(payload);
    const [value] = entry;
    if (value > filterValue) {
      break;
    } else if (value === filterValue) {
      entries.push(entry);
    }
  }
  return entries;
}

function parseIndexInteriorPage(page, pageType, numberOfCells, buffer) {
  let cursor = getPageHeaderSize(pageType);
  const keys = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    cursor += 2;
    let cellCursor = cellPointer;
    const page = buffer.readUInt32BE(cellCursor);
    cellCursor += 4;
    const { value: payloadSize, bytesRead } = readVarInt(buffer, cellCursor);
    cellCursor += bytesRead;
    const payload = buffer.subarray(cellCursor, cellCursor + payloadSize);
    const entry = parseIndexRow(payload);
    keys.push({ page, value: entry[0] });
  }
  return keys;
}

module.exports = {
  readIndexPage,
  readDatabaseHeader,
  readDatabaseSchemas,
  fetchPage,
  parsePageHeader,
  parseTableLeafPage,
  parseTableInteriorPage,
};
