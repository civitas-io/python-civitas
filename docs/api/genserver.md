# GenServer

OTP-style stateful service process for non-AI workloads: rate limiters, caches, coordinators.

See [GenServer](../genserver.md) for a full guide with examples.

---

::: civitas.genserver.GenServer
    options:
      members:
        - init
        - handle_call
        - handle_cast
        - handle_info
        - send_after
      show_source: true
