# Plugins

Structural protocols — implement the right methods and any class qualifies.
No base class, no registration decorator.

See [Plugins](../plugins.md) for a full authoring guide.

---

## ModelProvider

::: civitas.plugins.model.ModelProvider
    options:
      show_source: false

---

::: civitas.plugins.model.ModelResponse
    options:
      show_source: false

---

::: civitas.plugins.model.ToolCall
    options:
      show_source: false

---

## Implementations

Provider implementations (Anthropic, OpenAI, Gemini, Mistral, LiteLLM) live in
[civitas-contrib](https://github.com/civitas-io/civitas-contrib).
Install with e.g. `pip install civitas-contrib[anthropic]`.

---

## ToolProvider & ToolRegistry

::: civitas.plugins.tools.ToolProvider
    options:
      show_source: false

---

::: civitas.plugins.tools.ToolRegistry
    options:
      members:
        - register
        - get
        - all_tools
        - schemas
      show_source: true

---

## StateStore

::: civitas.plugins.state.StateStore
    options:
      show_source: false

---

::: civitas.plugins.state.InMemoryStateStore
    options:
      show_source: true

---

SQLiteStateStore and PostgresStateStore live in
[civitas-contrib](https://github.com/civitas-io/civitas-contrib).
Install with `pip install civitas-contrib` or `pip install civitas-contrib[postgres]`.

---

## Plugin Loading

::: civitas.plugins.loader.load_plugins_from_config
    options:
      show_source: true
