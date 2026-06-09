# claude-refs/

Lightweight Claude stubs propagated by `workspace/claude-sync` to every
repository in the workspace.

The files here are intentionally short. They redirect Claude to the
authoritative project context in `yoke-meta/claude/` rather than embedding
project-wide instructions in every repository. This avoids the drift that
inevitably appears when the same instructions live in many places.

## Propagation

`workspace/claude-sync` copies the contents of this directory into the
root of every locally-present workspace repository. Existing files with
identical content are left untouched; files that differ are overwritten.

Propagation is by file copy, not by symlink. Each repository owns its
copy and can be browsed standalone, without depending on the workspace
layout.

## Editing

The canonical copies live here. Edits made to a propagated copy in another
repository are overwritten on the next `claude-sync`. To change the stubs,
edit them in this directory and run `claude-sync`.

## Files

| File         | Where it lands in each repo                    |
| ------------ | ---------------------------------------------- |
| `CLAUDE.md`  | `<repo-root>/CLAUDE.md`                        |
