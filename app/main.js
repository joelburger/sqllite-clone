const { open } = require('fs/promises');
const path = require('path');
const handleSelect = require('./commands/select');
const handleTables = require('./commands/tables');
const handleDbInfo = require('./commands/dbinfo');
const { readDatabaseHeader, readDatabaseSchemas } = require('./database');

async function main() {
  const databaseFile = process.argv[2];
  const command = process.argv[3];

  let fileHandle;
  try {
    const filePath = path.join(process.cwd(), databaseFile);
    fileHandle = await open(filePath, 'r');
    const { pageSize } = await readDatabaseHeader(fileHandle);
    const { tables, indexes } = await readDatabaseSchemas(fileHandle, pageSize);

    if (command === '.dbinfo') {
      handleDbInfo(tables, pageSize);
    } else if (command === '.tables') {
      handleTables(tables);
    } else if (command.toUpperCase().startsWith('SELECT')) {
      await handleSelect(command, fileHandle, pageSize, tables, indexes);
    }
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    if (fileHandle) {
      await fileHandle.close();
    }
  }
}

main();
