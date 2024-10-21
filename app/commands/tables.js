function handle(tables) {
  const userTables = tables
    .map((table) => table.get('name'))
    .filter((tableName) => tableName !== 'sqlite_sequence')
    .sort()
    .join(' ');

  console.log(userTables);
}

module.exports = handle;
