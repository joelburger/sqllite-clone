const { parseSelectCommand, parseColumns, parseIndex } = require('../sqlparser');
const { logDebug } = require('../logger');
const {
  readIndexPage,
  parseTableLeafPage,
  parseTableInteriorPage,
  parsePageHeader,
  fetchPage,
  pageTypes,
} = require('../database');

function searchIndex(queryTableName, whereClause, indexes) {
  const [filterKey] = whereClause[0];
  return indexes.find((index) => {
    const { tableName, columns } = parseIndex(index.get('schemaBody'));
    return tableName === queryTableName && columns.includes(filterKey);
  });
}

function projectTableRows(rows, queryColumns) {
  return rows.map((row) => queryColumns.map((queryColumn) => row.get(queryColumn)).join('|'));
}

function filterRows(rows, whereClause) {
  if (whereClause.length === 0) {
    return rows;
  }
  const [filterColumn, filterValue] = whereClause[0];
  return rows.filter((row) => {
    return row.get(filterColumn) === filterValue;
  });
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

async function handle(command, fileHandle, pageSize, tables, indexes) {
  const { queryColumns, queryTableName, whereClause } = parseSelectCommand(command);
  const table = tables.find((table) => table.get('name') === queryTableName);
  if (!table) {
    throw new Error(`Table ${queryTableName} not found`);
  }
  const { columns, identityColumn } = parseColumns(table.get('schemaBody'));

  let indexData;
  if (whereClause.length > 0) {
    const index = searchIndex(queryTableName, whereClause, indexes);
    if (index) {
      const indexPage = index.get('rootPage');
      const [, filterValue] = whereClause[0];
      indexData = await readIndexPage(fileHandle, indexPage, pageSize, filterValue);
      logDebug('readIndexPage results', { indexData });
    }
  }

  let rows;
  if (indexData) {
    const startTime = Date.now();
    rows = await indexScan(fileHandle, table.get('rootPage'), pageSize, columns, identityColumn, indexData);
    logDebug('indexScan elapsed time', Date.now() - startTime);
  } else {
    rows = filterRows(
      await tableScan(fileHandle, table.get('rootPage'), pageSize, columns, identityColumn),
      whereClause,
    );
  }

  if (queryColumns[0] === 'count(*)') {
    console.log(rows.length);
  } else {
    const result = projectTableRows(rows, queryColumns);
    console.log(result.join('\n'));
  }
}

module.exports = handle;
