/**
 * Reads a variable-length integer from the buffer starting at the given offset.
 * The integer is encoded using a variable-length encoding scheme where each byte
 * contains 7 bits of the integer and the most significant bit (MSB) indicates if
 * there are more bytes to read.
 *
 * @param {Buffer} buffer - The buffer containing the encoded integer.
 * @param {number} offset - The offset in the buffer to start reading from.
 * @returns {Object} An object containing the decoded integer value and the number of bytes read.
 */
const readVarInt = (buffer, offset) => {
  let value = 0;
  let bytesRead = 0;
  for (let i = 0; i < 9; i += 1) {
    value |= (buffer[offset + i] & 0x7f) << (7 * i);
    bytesRead += 1;
    if (!(buffer[offset + i] & 0x80)) {
      break;
    }
  }
  return { value, bytesRead };
};

module.exports = readVarInt;
