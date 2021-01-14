const winston = require("winston");
const { combine, timestamp,colorize, printf} = winston.format;

const myFormat = printf(({ level, message, label, timestamp }) => {
  return `${timestamp} [${label}] ${level}: ${message}`;
});

const logger = winston.createLogger({
  handleExceptions: true,
  format: combine(
    timestamp(),
    colorize(),
    myFormat
  ),
  transports: [
    new winston.transports.Console(),
  ],
  exitOnError: false, 
});

module.exports = logger;