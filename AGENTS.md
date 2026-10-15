# Agent Rules — Context Efficiency

## Code Discovery (CRITICAL)

This repo has a semantic index (`open-codebase-index`).

## Codebase tools (MANDATORY)

Available tools from open-codebase-index:

Use such requests:

```js
return await tools.index_status({});
return await tools.codebase_context({ query: "query" });
return await tools.codebase_search({ query: "query" });
return await tools.codebase_peek({ query: "query" });
```

- codebase_context (Orient in unfamiliar repository code with a compact evidence pack)
- codebase_search (Retrieve full matching code)
- architecture_context (Obtain a brief repository map before targeted extraction or editing)
- codebase_peek (Find likely files and symbols without source bodies)
- implementation_lookup (Find an authoritative definition)
- find_similar (Find analogous implementations or duplicates)
- call_graph (Find direct callers or callees)

Call them by these exact names.
Do not invent tools.codebase_* paths.
Do not use Glob/Grep/Read for discovery when these tools exist.

**Only then** Read a specific file — and only the needed range if possible.

### Hard rules
- Do **not** open large files “just to look around”.
- Do **not** re-read the same file multiple times in one session.
- Prefer peek/context → targeted search → minimal Read.
- After finding the right location, edit — don’t keep loading more files.

## Context hygiene
- Prefer short, focused tool results over dumping whole modules.
- If you already have the relevant snippet, do not call Read again on that path.
- Summarize intermediate findings instead of carrying full tool outputs forward.