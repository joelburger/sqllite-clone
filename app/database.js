const { logDebug, logTrace } = require('./logger');
const readVarInt = require('./varint');

const DATABASE_HEADER_SIZE = 100;

const INDEX_COLUMNS = ['key', 'rowId'];

const SCHEMA_COLUMNS = ['schemaType', 'schemaName', 'name', 'rootPage', 'schemaBody'];

const pageTypes = {
  TABLE_LEAF: 0x0d,
  TABLE_INTERIOR: 0x05,
  INDEX_LEAF: 0x0a,
  INDEX_INTERIOR: 0x02,
};

function getPageHeaderSize(pageType) {
  if (pageType === pageTypes.INDEX_LEAF || pageType === pageTypes.TABLE_LEAF) {
    return 8;
  } else if (pageType === pageTypes.INDEX_INTERIOR || pageType === pageTypes.TABLE_INTERIOR) {
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

/**
 * Parses the header of a page from the database file.
 *
 * @param {Buffer} buffer - The buffer containing the page data.
 * @param {number} page - The page number being parsed.
 * @param {number} offset - The offset within the buffer where the page header starts.
 * @returns {Object} An object containing the parsed page header information:
 *                   - pageType: The type of the page (leaf or interior).
 *                   - startOfFreeBlock: The offset to the start of the first free block.
 *                   - numberOfCells: The number of cells on the page.
 *                   - startOfCellContentArea: The offset to the start of the cell content area.
 *                   - rightMostPointer: The right-most pointer (only for interior pages).
 *                   - pageHeaderSize: The size of the page header.
 * @throws {Error} If the page type is invalid.
 */
function parsePageHeader(buffer, page, offset) {
  const pageType = buffer.readInt8(offset);
  const startOfFreeBlock = buffer.readUInt16BE(offset + 1);
  const numberOfCells = buffer.readUInt16BE(offset + 3);
  const startOfCellContentArea = buffer.readUInt16BE(offset + 5);
  const rightMostPointer =
    pageType === pageTypes.INDEX_INTERIOR || pageType === pageTypes.TABLE_INTERIOR
      ? buffer.readUInt32BE(offset + 8)
      : undefined;
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

/**
 * Parses a table leaf page from the database file.
 *
 * @param {number} numberOfCells - The number of cells on the page.
 * @param {Buffer} buffer - The buffer containing the page data.
 * @param {Array<string>} columns - The columns to be parsed from the table.
 * @param {string} identityColumn - The identity column of the table.
 * @param {Array<Object>} [indexData] - Optional index data to filter the rows.
 * @returns {Array<Map<string, any>>} An array of rows parsed from the table leaf page.
 */
function parseTableLeafPage(numberOfCells, buffer, columns, identityColumn, indexData) {
  let cursor = getPageHeaderSize(pageTypes.TABLE_LEAF);
  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { payload, rowId } = readCellPayload(pageTypes.TABLE_LEAF, buffer, cellPointer);
    const row = parseRow(payload, columns);
    if (identityColumn) {
      row.set(identityColumn, rowId);
    }
    if (indexData) {
      const found = indexData.find((entry) => entry.get('rowId') === rowId);
      if (found) rows.push(row);
    } else {
      rows.push(row);
    }

    cursor += 2;
  }
  return rows;
}

/**
 * Parses a table interior page from the database file.
 *
 * @param {number} page - The page number being parsed.
 * @param {number} numberOfCells - The number of cells on the page.
 * @param {Buffer} buffer - The buffer containing the page data.
 * @returns {Array<Object>} An array of child pointers, each containing:
 *                         - page: The page number of the child.
 *                         - rowId: The row ID of the child.
 */
function parseTableInteriorPage(page, numberOfCells, buffer) {
  let cursor = getPageHeaderSize(pageTypes.TABLE_INTERIOR);
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

/**
 * Parses an index leaf page from the database file.
 *
 * @param {Buffer} buffer - The buffer containing the page data.
 * @param {number} numberOfCells - The number of cells on the page.
 * @param {number} filterValue - The value to filter the index entries.
 * @returns {Array<Map<string, any>>} An array of rows parsed from the index leaf page.
 */
function parseIndexLeafPage(buffer, numberOfCells, filterValue) {
  let cursor = getPageHeaderSize(pageTypes.INDEX_LEAF);

  const rows = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    cursor += 2;
    let cellCursor = cellPointer;
    const { value: payloadSize, bytesRead } = readVarInt(buffer, cellCursor);
    cellCursor += bytesRead;
    const payload = buffer.subarray(cellCursor, cellCursor + payloadSize);
    const row = parseRow(payload, INDEX_COLUMNS);
    const key = row.get('key');
    if (key > filterValue) {
      break;
    } else if (key === filterValue) {
      rows.push(row);
    }
  }
  return rows;
}

/**
 * Parses an index interior page from the database file.
 *
 * @param {Buffer} buffer - The buffer containing the page data.
 * @param {number} numberOfCells - The number of cells on the page.
 * @returns {Array<Object>} An array of keys, each containing:
 *                         - page: The page number of the child.
 *                         - value: The key value of the child.
 */
function parseIndexInteriorPage(buffer, numberOfCells) {
  let cursor = getPageHeaderSize(pageTypes.INDEX_INTERIOR);
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
    const row = parseRow(payload, INDEX_COLUMNS);
    keys.push({ page, value: row.get('key') });
  }
  return keys;
}

/**
 * Reads a value from the buffer based on the serial type.
 *
 * @param {Buffer} buffer - The buffer containing the data.
 * @param {number} cursor - The current cursor position in the buffer.
 * @param {number} serialType - The serial type indicating the data type and size.
 * @returns {Object} An object containing:
 *                   - value: The value read from the buffer.
 *                   - newCursor: The new cursor position after reading the value.
 * @throws {Error} If the serial type is unknown.
 */
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

/**
 * Reads the payload of a cell from the buffer.
 *
 * @param {number} pageType - The type of the page (leaf or interior).
 * @param {Buffer} buffer - The buffer containing the page data.
 * @param {number} cellPointer - The offset within the buffer where the cell starts.
 * @returns {Object} An object containing:
 *                   - payload: The payload data of the cell.
 *                   - rowId: The row ID of the cell (only for table pages).
 */
function readCellPayload(pageType, buffer, cellPointer) {
  let cursor = cellPointer;
  const { value: recordSize, bytesRead } = readVarInt(buffer, cursor);
  cursor += bytesRead;

  let rowId;
  if (pageType === pageTypes.TABLE_LEAF || pageType === pageTypes.TABLE_INTERIOR) {
    const { value, bytesRead: rowIdBytesRead } = readVarInt(buffer, cursor);
    rowId = value;
    cursor += rowIdBytesRead;
  }

  const startOfRecord = cursor;
  const endOfRecord = startOfRecord + recordSize;
  const payload = buffer.subarray(startOfRecord, endOfRecord);

  logTrace('readCell', {
    pageType,
    cellPointer,
    recordSize,
    bytesRead,
    rowId,
    first10Bytes: payload.subarray(0, 10),
    payload: payload.toString('utf8'),
  });

  return { payload, rowId };
}

async function readIndexData(fileHandle, page, pageSize, filterValue) {
  const buffer = await fetchPage(fileHandle, page, pageSize);
  const { pageType, numberOfCells, rightMostPointer } = parsePageHeader(buffer, page, 0);
  const results = [];
  if (pageType === pageTypes.INDEX_INTERIOR) {
    const keys = parseIndexInteriorPage(buffer, numberOfCells);
    for (const key of keys) {
      if (key.value >= filterValue) {
        const subresult = await readIndexData(fileHandle, key.page, pageSize, filterValue);
        if (subresult.length === 0) {
          break;
        }
        results.push(...subresult);
      }
    }
    if (rightMostPointer !== undefined) {
      const subresult = await readIndexData(fileHandle, rightMostPointer, pageSize, filterValue);
      results.push(...subresult);
    }
  } else if (pageType === pageTypes.INDEX_LEAF) {
    const indexData = parseIndexLeafPage(buffer, numberOfCells, filterValue);
    results.push(...indexData);
  }

  return results;
}

/**
 * Reads the database schemas from the database file.
 *
 * @param {FileHandle} fileHandle - The file handle to read from.
 * @param {number} pageSize - The size of each page in the database file.
 * @returns {Promise<Object>} A promise that resolves to an object containing:
 *                            - tables: An array of table schema rows.
 *                            - indexes: An array of index schema rows.
 * @throws {Error} If an invalid schema type is encountered.
 */
async function readDatabaseSchemas(fileHandle, pageSize) {
  const buffer = await fetchPage(fileHandle, 1, pageSize);
  const { pageType, numberOfCells, pageHeaderSize } = parsePageHeader(buffer, 1, DATABASE_HEADER_SIZE);
  let cursor = pageHeaderSize + DATABASE_HEADER_SIZE;
  const tables = [];
  const indexes = [];
  for (let i = 0; i < numberOfCells; i++) {
    const cellPointer = buffer.readUInt16BE(cursor);
    const { payload } = readCellPayload(pageType, buffer, cellPointer);
    const row = parseRow(payload, SCHEMA_COLUMNS);
    const schemaType = row.get('schemaType');
    if (schemaType === 'table') {
      tables.push(row);
    } else if (schemaType === 'index') {
      indexes.push(row);
    } else {
      throw new Error(`Invalid schema type: ${schemaType}`);
    }
    cursor += 2;
  }

  return { tables, indexes };
}

/**
 * Parses a row from the buffer based on the provided columns.
 *
 * @param {Buffer} buffer - The buffer containing the row data.
 * @param {Array<string>} columns - The columns to be parsed from the row.
 * @returns {Map<string, any>} A map representing the parsed row, where keys are column names and values are the corresponding data.
 */
function parseRow(buffer, columns) {
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

  logTrace('parseRow', { buffer, serialType, row });

  return row;
}

async function indexScan(fileHandle, page, pageSize, columns, identityColumn, indexData) {
  const buffer = await fetchPage(fileHandle, page, pageSize);
  const { pageType, numberOfCells, rightMostPointer } = parsePageHeader(buffer, page, 0);

  if (pageType === pageTypes.TABLE_LEAF) {
    return parseTableLeafPage(numberOfCells, buffer, columns, identityColumn, indexData);
  } else if (pageType === pageTypes.TABLE_INTERIOR) {
    const rows = [];
    const childPointers = parseTableInteriorPage(page, numberOfCells, buffer);
    const filteredChildPointers = filterChildPointers(childPointers, indexData);
    for (const childPointer of filteredChildPointers) {
      rows.push(...(await indexScan(fileHandle, childPointer.page, pageSize, columns, identityColumn, indexData)));
    }
    if (rightMostPointer) {
      rows.push(...(await indexScan(fileHandle, rightMostPointer, pageSize, columns, identityColumn, indexData)));
    }
    return rows;
  }
  throw new Error(`Unknown page type: ${pageType}`);
}

async function tableScan(fileHandle, page, pageSize, columns, identityColumn) {
  const buffer = await fetchPage(fileHandle, page, pageSize);
  const { pageType, numberOfCells, rightMostPointer } = parsePageHeader(buffer, page, 0);

  if (pageType === pageTypes.TABLE_LEAF) {
    return parseTableLeafPage(numberOfCells, buffer, columns, identityColumn);
  } else if (pageType === pageTypes.TABLE_INTERIOR) {
    const rows = [];
    const childPointers = parseTableInteriorPage(page, numberOfCells, buffer);

    for (const childPointer of childPointers) {
      rows.push(...(await tableScan(fileHandle, childPointer.page, pageSize, columns, identityColumn)));
    }

    if (rightMostPointer) {
      rows.push(...(await tableScan(fileHandle, rightMostPointer, pageSize, columns, identityColumn)));
    }

    return rows;
  }
  throw new Error(`Unknown page type: ${pageType}`);
}

function filterChildPointers(childPointers, indexData) {
  const result = new Set();
  for (const index of indexData) {
    const rowId = index.get('rowId');

    let fromChildPointer, toChildPointer;
    for (const childPointer of childPointers) {
      if (rowId >= childPointer.rowId) {
        fromChildPointer = childPointer;
      }
      if (rowId <= childPointer.rowId) {
        toChildPointer = childPointer;
        break;
      }
    }
    if (fromChildPointer) result.add(fromChildPointer);
    if (toChildPointer) result.add(toChildPointer);
  }
  const resultArray = Array.from(result);
  logDebug('filtered child pointers', resultArray);
  return resultArray;
}

module.exports = {
  readIndexData,
  readDatabaseHeader,
  readDatabaseSchemas,
  indexScan,
  tableScan,
};
