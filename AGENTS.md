# Agent Rules — Context Efficiency

## Code Discovery (CRITICAL)

This repo has a semantic index (`open-codebase-index`).

**ALWAYS prefer index tools over raw file reads:**

1. Unknown area / “how does X work?”  
   → `codebase_context` (compact, tokenBudget ≈ 600) **or** `codebase_peek`

2. Need actual code snippets  
   → `codebase_search` (not full-file Read)

3. Know the symbol name  
   → `implementation_lookup` / `call_graph`

4. Exact string / regex  
   → `grep` / `rg`

5. **Only then** Read a specific file — and only the needed range if possible.

### Hard rules
- Do **not** open large files “just to look around”.
- Do **not** re-read the same file multiple times in one session.
- Prefer peek/context → targeted search → minimal Read.
- After finding the right location, edit — don’t keep loading more files.

## Context hygiene
- Prefer short, focused tool results over dumping whole modules.
- If you already have the relevant snippet, do not call Read again on that path.
- Summarize intermediate findings instead of carrying full tool outputs forward.