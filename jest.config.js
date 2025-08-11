module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/tests/**/*.test.js'],
  collectCoverage: true,
  collectCoverageFrom: ['src/**/*.js', '!src/main.js'],
  coverageDirectory: 'coverage',
  verbose: true,
};


