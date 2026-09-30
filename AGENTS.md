## Graphify

If a local knowledge graph has been generated, it lives at graphify-out/.
Generated graph files are not included in the portfolio snapshot.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:

- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify cluster-only .` to keep the graph current (AST-only, no API cost)



## Documentation

Always apply these documentation rules to code in this project.

#### Language

- Write documentation prose in English.
- Keep technical names, identifiers, endpoints, classes, methods, types, and concepts whose official name is English in English.
- Do not invent features, guarantees, metrics, or results that the code does not demonstrate.



#### Python

- Use NumPy-style docstrings.
- Always use these section headers, without mixing or translating them:
  - Parameters
  - Returns
  - Raises
  - Attributes
  - Invariants
  - Notes
  - Examples
  - Yields
- Include only the applicable sections.
- Document modules, classes, functions, and public methods.
- Document private methods only when they contain non-obvious logic, business rules, calculations, side effects, or important decisions.
- Do not document getters, setters, wrappers, or trivial functions whose behavior is already obvious from the name, types, and body.
- Do not literally repeat type annotations unless they add context.
- In Parameters, explain purpose, units, valid values, and relevant defaults.
- In Returns, explain the shape and meaning of the result.
- In Raises, document only exceptions the code can actually raise.
- In Notes, include only invariants, side effects, fallbacks, concurrency, persistence, complexity, or relevant technical decisions.
- For entities and value objects, document identity, mutability, invariants, units, and validation rules.
- For adapters, document the implemented contract, the external source, fallbacks, and I/O effects.
- For endpoints, document purpose, relevant HTTP parameters, response, error codes, and external dependencies.
- For quantitative strategies, document the formula, decision rule, minimum required data, and signal meaning.
- Use equations only when they clarify a real rule in the code.
- Do not add marketing claims, backtesting results, or invented complexity.



#### Module template

```python
"""
Brief summary of the module's responsibility.

Concise explanation of its role and boundaries.

Responsibilities
----------------
- Relevant responsibility.

Notes
-----
- Important invariant, side effect, or technical decision.
"""
```



##### Class template

```python
class Example:
    """
    Describe what the class represents and its responsibility.

    Attributes
    ----------
    attribute : type
        Meaning of the attribute.

    Invariants
    ----------
    - Rule that must always hold.

    Notes
    -----
    - Relevant technical decision or side effect.
    """
```



##### Function / method template

```python
def example(value: str, limit: int = 10) -> list[str]:
    """
    Describe the action with a clear verb.

    Parameters
    ----------
    value : str
        Input value and relevant constraints.
    limit : int, default=10
        Limit applied to the result.

    Returns
    -------
    list[str]
        Result produced by the function.

    Raises
    ------
    ValueError
        If the value fails required validation.

    Notes
    -----
    - Only if there is an important decision, side effect, or edge case.
    """
```



#### TypeScript / JavaScript

- Use JSDoc `/** ... */` for exported functions, hooks, classes, types, and interfaces when their behavior is not obvious.
- Write the text in English.
- Use `@param`, `@returns`, `@throws`, and `@example` only when needed.
- Document React components with a brief sentence and document their props only when they are not obvious.
- Do not fill JSX with explanatory comments.
- Use `//` comments only for non-obvious decisions, algorithms, fallbacks, or technical limits.
- Do not use comments to describe obvious visual labels such as “Header”, “Button”, “Price”, or “Section”.
- Keep a single naming and tone convention across all files.



### General rules

- Update the docstring when the documented behavior changes.
- The docstring must describe the current code, not future intent.
- Do not make docstrings artificially long.
- Do not duplicate documentation across module, class, and method.
- Do not add documentation to generated files, locks, builds, dependencies, or configuration unless there is a non-obvious operational decision.
- Before finishing, check that there is no mix of `Args`, `Parameters`, `Parámetros`, `Returns`, `Retorna`, `Raises`, or `Lanza`.
- Keep the project's existing format only when it matches these rules; if it conflicts, apply these rules.

