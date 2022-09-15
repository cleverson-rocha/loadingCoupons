const randomString = require('randomstring');

const stringCodeGenerator = () => {
  let alphanumeric = randomString.generate({
    length: 12,
    charset: 'alphanumeric'
  });

  code = alphanumeric.toUpperCase()
  // console.log(code)
};

module.exports = {
  stringCodeGenerator
};