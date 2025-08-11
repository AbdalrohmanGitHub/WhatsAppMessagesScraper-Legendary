const {
  matchesAny,
  matchesNone,
  patternMatches,
  selectedChatMatcher,
  extractLinks,
  approxBytesFromBase64,
  extFromMime,
  applyRedaction,
} = require('../src/utils');

describe('text pattern helpers', () => {
  test('matchesAny substring', () => {
    expect(matchesAny('hello world', ['world'])).toBe(true);
    expect(matchesAny('hello', ['world'])).toBe(false);
  });
  test('matchesAny regex', () => {
    expect(matchesAny('Price: $123', ['/\\$\\d+/'])).toBe(true);
  });
  test('matchesNone', () => {
    expect(matchesNone('hello world', ['abc'])).toBe(true);
    expect(matchesNone('hello world', ['world'])).toBe(false);
  });
  test('patternMatches', () => {
    expect(patternMatches('Team Chat', 'name:Team')).toBe(false);
    expect(patternMatches('Team Chat', 'Team')).toBe(true);
    expect(patternMatches('Order 123', '/\\d+/')).toBe(true);
  });
});

describe('chat selection', () => {
  const chat = { id: { _serialized: '1203@g.us', user: '1203' }, name: 'Marketing EU' };
  test('selectedChatMatcher id match', () => {
    expect(selectedChatMatcher(chat, ['id:1203'])).toBe(true);
  });
  test('selectedChatMatcher name match', () => {
    expect(selectedChatMatcher(chat, ['name:Marketing'])).toBe(true);
  });
  test('selectedChatMatcher fallback', () => {
    expect(selectedChatMatcher(chat, ['EU'])).toBe(true);
    expect(selectedChatMatcher(chat, ['CN'])).toBe(false);
  });
});

describe('links and media helpers', () => {
  test('extractLinks', () => {
    const t = 'See https://example.com and http://foo.bar?q=1';
    const links = extractLinks(t);
    expect(links.length).toBe(2);
  });
  test('approxBytesFromBase64', () => {
    const bytes = approxBytesFromBase64('QUJD'); // ABC
    expect(bytes).toBeGreaterThan(0);
  });
  test('extFromMime', () => {
    expect(extFromMime('image/jpeg')).toBe('jpeg');
    expect(extFromMime('text/plain')).toBe('plain');
  });
});

describe('redaction', () => {
  test('mask last 2 chars', () => {
    const r = applyRedaction('John', 'name', 'mask', true, false);
    expect(r.endsWith('hn')).toBe(true);
  });
  test('hash redaction', () => {
    const r = applyRedaction('123456789', 'number', 'hash', false, true);
    expect(r.startsWith('red_')).toBe(true);
  });
});


