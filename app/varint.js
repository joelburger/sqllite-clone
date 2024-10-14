const readVarInt = (buffer, offset) => {
  let value = 0;
  let bytesRead = 0;
  let byte;
  for (let i = 0; i < 9; i++) {
    byte = buffer[offset + i];
    value <<= 7;
    value |= byte & 0x7f;
    bytesRead += 1;
    if (!(byte & 0x80)) {
      break;
    }
  }
  return { value, bytesRead };
};

module.exports = readVarInt;
