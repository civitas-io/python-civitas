# AgentProcess

Base class for all agents. Subclass it and implement `handle()`.

See [Core Concepts](../concepts.md) and [Getting Started](../getting-started.md) for usage examples.

---

::: civitas.process.AgentProcess
    options:
      members:
        - on_start
        - handle
        - on_error
        - on_stop
        - on_child_terminated
        - send
        - ask
        - broadcast
        - reply
        - checkpoint
        - spawn
        - despawn
        - stop
      show_source: true

---

::: civitas.process.ProcessStatus
    options:
      show_source: false

---

::: civitas.process.Mailbox
    options:
      show_source: true
