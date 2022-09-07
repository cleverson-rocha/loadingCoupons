const randomString = require('randomstring');

function createCodeAlphanumeric() {
  let alphanumeric = randomString.generate({
    length: 12,
    charset: 'alphanumeric'
  });

  code = alphanumeric.toUpperCase()
};

createCodeAlphanumeric()

// console.log(code)

module.exports = { randomString }