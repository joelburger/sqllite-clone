function parseWhereClause(whereClause) {
  if (whereClause) {
    return [whereClause.split('=').map((value) => value.trim().replaceAll("'", ''))];
  }
  return [];
}

function parseSelectCommand(command) {
  const pattern =
    /select\s+(?<columns>[\w\(\)\*,\s]+)\s+from\s+(?<tableName>[\w]+)(\s+where\s+(?<whereClause>[\w\s=\']+))*/i;
  const result = pattern.exec(command);

  const queryColumns = result?.groups['columns'].split(',').map((column) => column.trim());
  const queryTableName = result?.groups['tableName'];
  const whereClause = parseWhereClause(result?.groups['whereClause']);

  return { queryTableName, queryColumns, whereClause };
}

module.exports = {
  parseSelectCommand,
};
