# Runtime

Assembles and manages the full Civitas runtime. Wires transport, registry,
serializer, tracer, plugins, and the supervision tree.

See [Deployment](../deployment.md) and [Topology & CLI](../topology.md) for usage.

---

::: civitas.runtime.Runtime
    options:
      members:
        - from_config
        - start
        - stop
        - ask
        - send
        - get_agent
        - all_agents
        - print_tree
        - spawn
        - despawn
        - stop_agent
      show_source: true

---

::: civitas.components.ComponentSet
    options:
      members:
        - inject
      show_source: true

---

::: civitas.components.build_component_set
    options:
      show_source: true
