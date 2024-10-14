const readVarInt = require('./varint');

describe('varint tests', () => {
  test('readVarInt test 1', () => {
    // arrange
    const buffer = Buffer.from([0x2c, 0x06, 0x04, 0x00]);

    // act
    const { value: actual } = readVarInt(buffer, 0);

    // assert
    expect(actual).toEqual(44);
  });

  test('readVarInt test 2', () => {
    // arrange
    const buffer = Buffer.from([0x81, 0x66, 0x08, 0x07]);

    // act
    const { value: actual } = readVarInt(buffer, 0);

    // assert
    expect(actual).toEqual(230);
  });

  test('readVarInt test 3', () => {
    // arrange
    const buffer = Buffer.from([0x1b]);

    // act
    const { value: actual } = readVarInt(buffer, 0);

    // assert
    expect(actual).toEqual(230);
  });
});
