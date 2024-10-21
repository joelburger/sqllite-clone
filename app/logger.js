const DEBUG_MODE = process.env.DEBUG_MODE;
const TRACE_MODE = process.env.TRACE_MODE;

function logDebug(...message) {
  if (DEBUG_MODE || TRACE_MODE) {
    console.log(...message);
  }
}

function logTrace(...message) {
  if (TRACE_MODE) {
    console.log(...message);
  }
}

module.exports = {
  logDebug,
  logTrace,
};
