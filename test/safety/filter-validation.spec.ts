import { BadRequestException } from '@nestjs/common';
import { parseListFilter } from '../../src/shared/filter/opportunity-filter';

/**
 * The safety mechanism: a filter the code does not understand must be rejected,
 * never ignored. A silently dropped ownerId would make a bulk job move every
 * opportunity in the workspace rather than the intended subset, and still report
 * success. Delete the KNOWN_PARAMS guard in opportunity.filter.ts and every case
 * in "rejects unrecognised input" goes red.
 */
describe('filter validation', () => {
  describe('rejects unrecognised input', () => {
    it.each([
      ['typo in a known param', { stageID: 'x' }],
      ['unsupported filter', { sortBy: 'value' }],
      ['a plausible but unimplemented one', { status: 'won' }],
      ['an injection attempt', { stageId: "x' OR 1=1--" }],
    ])('rejects %s', (_label, query) => {
      expect(() => parseListFilter(query as Record<string, unknown>)).toThrow(BadRequestException);
    });

    it('names the offending param and the supported set', () => {
      expect(() => parseListFilter({ sortBy: 'value' })).toThrow(/unknown filter "sortBy"/);
      expect(() => parseListFilter({ sortBy: 'value' })).toThrow(/supported:/);
    });
  });

  describe('validates each dimension', () => {
    it('rejects a non-uuid stageId', () => {
      expect(() => parseListFilter({ stageId: 'not-a-uuid' })).toThrow(BadRequestException);
    });

    it('rejects a uuid list containing one bad entry', () => {
      const good = '11111111-1111-1111-1111-111111111111';
      expect(() => parseListFilter({ stageId: `${good},nope` })).toThrow(BadRequestException);
    });

    it('rejects an outcome outside the enum', () => {
      expect(() => parseListFilter({ outcome: 'sideways' })).toThrow(/must be one of/);
    });

    it.each(['open', 'won', 'lost', 'abandoned'])('accepts outcome=%s', (outcome) => {
      expect(parseListFilter({ outcome }).outcome).toBe(outcome);
    });

    it('rejects a non-numeric bound', () => {
      expect(() => parseListFilter({ minValue: 'abc' })).toThrow(BadRequestException);
    });

    it('rejects an unparseable date', () => {
      expect(() => parseListFilter({ createdFrom: 'notadate' })).toThrow(/ISO-8601/);
    });

    it('rejects a non-integer or non-positive limit', () => {
      expect(() => parseListFilter({ limit: '0' })).toThrow(BadRequestException);
      expect(() => parseListFilter({ limit: '1.5' })).toThrow(BadRequestException);
      expect(() => parseListFilter({ limit: '-1' })).toThrow(BadRequestException);
    });

    it('caps limit rather than trusting it', () => {
      expect(parseListFilter({ limit: '100000' }).limit).toBe(200);
    });
  });

  describe('rejects contradictory ranges', () => {
    it('minValue above maxValue', () => {
      expect(() => parseListFilter({ minValue: '999', maxValue: '1' })).toThrow(
        /minValue must be less than or equal/,
      );
    });

    it('createdFrom after createdTo', () => {
      expect(() => parseListFilter({ createdFrom: '2026-01-01', createdTo: '2020-01-01' })).toThrow(
        /createdFrom must be earlier/,
      );
    });
  });

  describe('accepts the shapes the bulk filter needs', () => {
    const a = '11111111-1111-1111-1111-111111111111';
    const b = '22222222-2222-2222-2222-222222222222';

    it('a single stage', () => {
      expect(parseListFilter({ stageId: a }).stageIds).toEqual([a]);
    });

    it('a comma-separated stage list', () => {
      expect(parseListFilter({ stageId: `${a},${b}` }).stageIds).toEqual([a, b]);
    });

    it('every dimension at once', () => {
      const filter = parseListFilter({
        stageId: a,
        ownerId: b,
        outcome: 'won',
        minValue: '100',
        maxValue: '900',
        createdFrom: '2026-01-01',
        createdTo: '2026-06-01',
        limit: '25',
      });
      expect(filter).toMatchObject({
        stageIds: [a],
        ownerIds: [b],
        outcome: 'won',
        minValue: 100,
        maxValue: 900,
        limit: 25,
      });
      expect(filter.createdFrom).toEqual(new Date('2026-01-01'));
    });

    it('an empty filter still yields a usable default', () => {
      expect(parseListFilter({}).limit).toBe(50);
    });

    it('treats an empty string as absent, not as a bad value', () => {
      expect(parseListFilter({ stageId: '', ownerId: '' }).stageIds).toBeUndefined();
    });

    // The list endpoint receives query strings; the bulk-move endpoint receives
    // a JSON body where numbers stay numbers. Both must parse identically.
    it('accepts numbers, not just strings, so a JSON body filters correctly', () => {
      expect(parseListFilter({ minValue: 100 }).minValue).toBe(100);
      expect(parseListFilter({ minValue: 100 }).minValue).toBe(
        parseListFilter({ minValue: '100' }).minValue,
      );
      expect(parseListFilter({ maxValue: 0 }).maxValue).toBe(0);
    });

    it('rejects an explicit null rather than treating it as no bound', () => {
      // A JSON body can carry {"minValue": null}. Dropping it would run the job
      // over every matching row instead of the requested subset.
      expect(() => parseListFilter({ minValue: null })).toThrow(BadRequestException);
      expect(() => parseListFilter({ minValue: null })).toThrow(/must not be null/);
    });
  });
});
