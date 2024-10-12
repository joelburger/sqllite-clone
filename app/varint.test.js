const readVarInt = require('./varint');

describe('varint tests', () => {
  test('readVarInt', () => {
    // arrange
    const buffer = Buffer.from([0x2c, 0x06, 0x04, 0x00]);

    // act
    const { value: actual } = readVarInt(buffer, 0);

    // assert
    expect(actual).toEqual(44);
  });
});
