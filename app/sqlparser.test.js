const { parseSelectCommand, parseColumns, parseIndex } = require('./sqlparser');

describe('parser tests', () => {
  test('parses simple SELECT command', () => {
    const command = 'SELECT column1, column2 FROM tableName';
    const actual = parseSelectCommand(command);
    expect(actual).toEqual({
      queryTableName: 'tableName',
      queryColumns: ['column1', 'column2'],
      whereClause: [],
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
  test('parses columns from create table sql with primary key', () => {
    const createSql = 'CREATE TABLE oranges (id integer primary key autoincrement, name text, description text)';
    const { columns, identityColumn } = parseColumns(createSql);
    expect(columns).toEqual(['id', 'name', 'description']);
    expect(identityColumn).toEqual('id');
  });
  test('parses columns from create table sql without primary key', () => {
    const createSql = 'CREATE TABLE oranges (name text, description text)';
    const { columns, identityColumn } = parseColumns(createSql);
    expect(columns).toEqual(['name', 'description']);
    expect(identityColumn).toBeUndefined();
  });
  test('parses indexes from create index with single column', () => {
    const createSql = 'CREATE INDEX idx_companies_country on companies (country)';
    const { columns, tableName } = parseIndex(createSql);
    expect(tableName).toEqual('companies');
    expect(columns).toEqual(['country']);
  });
  test('parses indexes from create index with multiple columns', () => {
    const createSql = 'CREATE INDEX idx_companies_country on companies (country, industry)';
    const { columns, tableName } = parseIndex(createSql);
    expect(tableName).toEqual('companies');
    expect(columns).toEqual(['country', 'industry']);
  });
});
