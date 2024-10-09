const { parseSelectCommand } = require('./sqlparser');

describe('parseSelectCommand', () => {
  test('parses simple SELECT command', () => {
    const command = 'SELECT column1, column2 FROM tableName';
    const actual = parseSelectCommand(command);
    expect(actual).toEqual({
      queryTableName: 'tableName',
      queryColumns: ['column1', 'column2'],
    });
  });
  test('parses simple SELECT command with WHERE clause', () => {
    const command = "SELECT column1, column2 FROM tableName WHERE column1 = 'value1'";
    const actual = parseSelectCommand(command);
    expect(actual).toEqual({
      queryTableName: 'tableName',
      queryColumns: ['column1', 'column2'],
      whereClause: [['column1', 'value1']],
    });
  });
});
