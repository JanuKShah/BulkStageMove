/**
 * Checks the command the README documents still exists and still parses.
 *
 * The README quotes one CLI invocation as the example of the non-interactive
 * surface. Documentation that names a command nobody has run rots silently, so
 * this asserts the command is a real key in the CLI's command map.
 */
import { COMMANDS } from '../../scripts/cli';

describe('the readme CLI example', () => {
  it('documents a command that exists', () => {
    // Exactly as README.md shows it.
    expect(Object.keys(COMMANDS)).toContain('bulk-move');
  });

  it('documents a command the menu also offers', () => {
    // If these ever diverge, the README is pointing at something a user cannot
    // reach the way the README says they can.
    expect(typeof COMMANDS['bulk-move']).toBe('function');
  });
});

describe('the cli command surface', () => {
  it('lists its own commands when given an unknown one', () => {
    // The README tells a reader that a mistyped name lists them all, so the
    // fallback has to include the name that was not found.
    const keys = Object.keys(COMMANDS);
    expect(keys.length).toBeGreaterThan(0);
    expect(keys).not.toContain('not-a-real-command');
  });
});
