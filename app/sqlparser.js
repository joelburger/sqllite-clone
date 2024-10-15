function parseWhereClause(whereClause) {
  if (whereClause) {
    return [whereClause.split('=').map((value) => value.trim().replaceAll("'", ''))];
  }
  return [];
}

function parseSelectCommand(command) {
  const pattern = /select\s+(?<columns>[\w()*,\s]+)\s+from\s+(?<tableName>\w+)(\s+where\s+(?<whereClause>[\w\s=']+))*/i;
  const result = pattern.exec(command);

  const queryColumns = result?.groups['columns'].split(',').map((column) => column.trim());
  const queryTableName = result?.groups['tableName'];
  const whereClause = parseWhereClause(result?.groups['whereClause']);

  return { queryTableName, queryColumns, whereClause };
}

function extractFirstEntry(values) {
  return values?.trim().split(' ')[0];
}

function parseColumns(tableSchema) {
  const pattern = /^CREATE\s+TABLE\s+[\w"]+\s*\(\s*(?<columns>[\s\S_]+)\s*\)$/i;
  const matched = pattern.exec(tableSchema)?.groups.columns || '';

  if (!matched) {
    throw new Error(`Failed to parse columns from "${tableSchema}".`);
  }

  const columns = matched.split(',');
  const [identityColumn] = columns.filter((column) => column.toLowerCase().includes('integer primary key'));

  return {
    columns: columns.map((column) => extractFirstEntry(column)),
    identityColumn: extractFirstEntry(identityColumn),
  };
}

module.exports = {
  parseSelectCommand,
  parseColumns,
};
