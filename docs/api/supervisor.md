# Supervisor

Monitors child agents and supervisors. Applies restart strategies on failure.

See [Supervision](../supervision.md) for a full guide.

---

::: civitas.supervisor.Supervisor
    options:
      members:
        - start
        - stop
        - add_remote_child
        - all_agents
        - all_supervisors
      show_source: true

---

::: civitas.supervisor.RestartStrategy
    options:
      show_source: false

---

::: civitas.supervisor.BackoffPolicy
    options:
      show_source: false

---

# DynamicSupervisor

Starts empty. Children are added and removed at runtime via `self.spawn()` / `self.despawn()`. Always uses `ONE_FOR_ONE`. See [Dynamic supervision](../supervision.md#dynamic-supervision) for a full guide.

---

::: civitas.supervisor.DynamicSupervisor
    options:
      members:
        - on_spawn_requested
        - all_dynamic_agents
        - on_stop
      show_source: true

---

::: civitas.supervisor.RestartMode
    options:
      show_source: false
