const randomString = require('randomstring');

const codeGenerator = () => {
  let alphanumeric = randomString.generate({
    length: 19,
    charset: 'alphanumeric'
  });

  code = alphanumeric
};

// codeGenerator()

module.exports = {
  codeGenerator
};

// console.log(code)