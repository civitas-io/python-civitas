# HTTP Gateway

Supervised process that bridges external HTTP traffic onto the Civitas message bus.

See [HTTP Gateway](../gateway.md) for a full guide with examples.

---

::: civitas.gateway.core.GatewayConfig
    options:
      show_source: false

---

::: civitas.gateway.core.HTTPGateway
    options:
      members:
        - on_start
        - on_stop
        - handle
      show_source: true

---

::: civitas.gateway.router.RouteTable
    options:
      members:
        - from_config
        - from_class
        - merge_contracts_from
        - match
        - entries
      show_source: true

---

::: civitas.gateway.router.RouteEntry
    options:
      show_source: false

---

::: civitas.gateway.types.GatewayRequest
    options:
      show_source: false

---

::: civitas.gateway.types.GatewayResponse
    options:
      show_source: false

---

::: civitas.gateway.router.route
    options:
      show_source: true

---

::: civitas.gateway.contracts.contract
    options:
      show_source: true
