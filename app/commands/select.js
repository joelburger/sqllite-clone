const { parseSelectCommand, parseColumns, parseIndex } = require('../sqlparser');
const { logDebug } = require('../logger');
const { readIndexData, tableScan, indexScan } = require('../database');

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
      indexData = await readIndexData(fileHandle, indexPage, pageSize, filterValue);
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
