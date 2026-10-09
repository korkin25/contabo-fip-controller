# contabo-fip-controller

This controller keeps a [Contabo](https://contabo.com) floating IPv4 on a
Kubernetes node that is actually serving it.

The Contabo public network is routed. Gratuitous ARP from a VPS does not
reach the gateway, so keepalived, MetalLB L2 and Cilium L2 announcements
cannot move a public address. The only way to move a floating IP is the
Contabo API (`POST /v1/vips/{ip}/instances/{id}`). This controller makes that
API call when the node that holds the IP stops serving.

A node is **eligible** when every watched Service answers `200` on the
node's `healthCheckNodePort`. These Services are `type: LoadBalancer` with
`externalTrafficPolicy: Local`. kube-proxy and Cilium answer `200` on that
port only while the node has a ready local endpoint. ETP Local keeps the
client source IP. The price is that only nodes running a ready backend can
take the traffic, and this controller enforces exactly that.

## Behaviour

- The controller probes every node every `--interval` (2s) on its
  `InternalIP`, and immediately after Node or Service changes. Each result
  is debounced with `--rise`/`--fall`.
- **Sticky.** If the current holder is eligible, nothing happens.
- Otherwise the IP is assigned to the first eligible node by name.
  - **Unassign fallback.** If the assign is refused by the API itself — a
    4xx answer other than `401`, `404` and `429` — the controller re-reads
    the holder from the API (never from its cache). If the API already
    reports the target, the move counts as done. If another instance holds
    the IP, the controller unassigns that live holder and assigns again.
    Transport errors, timeouts, cancellation, `5xx`, `401`, `404` and `429`
    never lead to an unassign. This is the only code path that unassigns.
    The unassign+assign pair is not cancelled by shutdown and is bounded
    by 45s, so a SIGTERM cannot leave the IP unassigned between the calls.
  - Every move is checked by reading the VIP back for up to
    `--verify-timeout`. Failures back off exponentially from `--interval`
    up to `--max-backoff`. The count starts over whenever the decision
    changes (another target, or no move needed).
  - **Quarantine.** After 3 consecutive failed moves to one instance, it is
    not chosen as a target for `--quarantine` (5m), so the next eligible
    node by name gets a chance. A quarantined holder still keeps the IP
    while it is eligible.
- **Never unassigns** when no node is eligible. Pulling the IP from a node
  that might still serve some traffic does not help anyone.
- **Warm-up.** Probe state starts unhealthy after a start or a leader
  takeover. Until `--rise` + `--fall` probe rounds have completed, an
  assigned IP is not moved away from its holder, so a holder that missed
  its first probes keeps the IP. An unassigned IP is assigned as soon as a
  node is eligible.
- The holder is cached and re-read from the API every `--vip-refresh` (30s).
  Failed reads back off like failed moves. The API client also answers a
  refused token grant from cache for 30s, so rotated credentials do not
  hammer the auth server (and risk a lockout) every interval.
- Nodes are skipped when they:
  - lack the `contabo.com/instance-id` label;
  - have a label value that is not a positive decimal integer without
    leading zeros (logged once per node and value);
  - carry `node.kubernetes.io/exclude-from-external-load-balancers`;
  - are being deleted.
- Two replicas run with Lease leader election, and only the leader acts.
  On SIGTERM the leader stops its loop, waits for an in-flight move, and
  only then releases the Lease. A replica that loses the Lease exits and
  restarts with clean state.
- On every move the controller records Events on the watched Services:
  `Reassigned`, `ReassignFailed`, `TargetQuarantined`, `DryRunReassign`.

## Requirements

- Every node is labelled with its Contabo instance ID. With Talos:

  ```yaml
  machine:
    nodeLabels:
      contabo.com/instance-id: "203650802"
  ```

- The controller pods can reach each node's `InternalIP` on the
  health-check node ports (the Talos/host firewall must allow it).
- How the floating IP reaches the Service on the node, for example
  `spec.externalIPs` or LB-IPAM with Cilium, is up to the cluster. The
  controller only moves the IP.
- A Contabo API user restricted to the VIP permissions:
  - `/v1/vips` — READ
  - `/v1/vips/:ip` — READ
  - `/v1/vips/:ip/:resourceType/:resourceId` — CREATE, DELETE

## Configuration

The credentials come from the environment variables `CONTABO_CLIENT_ID`,
`CONTABO_CLIENT_SECRET`, `CONTABO_API_USER` and `CONTABO_API_PASSWORD`.
`deploy/example` projects them from Vault with External Secrets Operator.
Restart the pods after you rotate the credentials.

| Flag | Default | |
|---|---|---|
| `--floating-ip` | — | floating IPv4 to manage (required) |
| `--service` | — | `namespace/name`, repeatable or comma-separated (required) |
| `--instance-label` | `contabo.com/instance-id` | node label with the instance ID |
| `--node-address-type` | `InternalIP` | node address to probe |
| `--interval` | `2s` | reconcile interval |
| `--rise` / `--fall` | `2` / `2` | debounce thresholds |
| `--probe-timeout` | `1s` | timeout of one probe |
| `--vip-refresh` | `30s` | re-read the holder from the API |
| `--verify-timeout` | `15s` | wait for the API to report the new holder |
| `--max-backoff` | `1m` | cap on retry delay of moves and VIP reads |
| `--quarantine` | `5m` | how long an instance is skipped as a target after 3 failed moves |
| `--dry-run` | `false` | decide and emit Events, never write to the API |
| `--leader-elect` | `true` | Lease `--lease-name` in `--lease-namespace` (`POD_NAMESPACE`) |
| `--metrics-addr` | `:8080` | `/metrics`, `/healthz` |

All durations must be positive and `--node-address-type` must be one of
`Hostname`, `InternalIP`, `ExternalIP`, `InternalDNS`, `ExternalDNS`. A
Service named twice in `--service` is watched once.

Deploy with `kubectl apply -k deploy/example` after you edit the overlay.
The base sets `GOMEMLIMIT` from the container memory limit, a
PodDisruptionBudget with `minAvailable: 1`, and a 60s termination grace
period that covers an in-flight move.

## Health

`/healthz` answers `200` on followers. On the replica that runs the loop it
answers `500` when no cycle has succeeded for
`max(10 × --interval, --verify-timeout + --max-backoff + --interval)` (77s
with the defaults), counted from the start of the loop. A cycle succeeds
when it evaluated every node, the last VIP read worked and it is not
retrying a failed move. The liveness probe then restarts the pod: the
other replica takes the Lease, and the new pod reads the credentials from
the Secret again. The metrics server starts before the informer caches
sync; if it cannot listen, the process exits non-zero.

## Metrics

Every replica exports `contabo_fip_leader` and the counters. The state
metrics marked *leader* exist only on the replica that runs the loop; a
follower does not export them at all, and a replica that stops leading
drops them.

| Metric | Meaning |
|---|---|
| `contabo_fip_leader` | `1` on the replica that holds the Lease, `0` elsewhere |
| `contabo_fip_node_eligible{node}` | per-node verdict (leader) |
| `contabo_fip_eligible_nodes` | number of eligible nodes (leader) |
| `contabo_fip_holder{node,instance_id}` | the current holder; `node` is empty when the holder is not a known node (leader) |
| `contabo_fip_last_reconcile_timestamp_seconds` | Unix time of the last successful cycle (leader) |
| `contabo_fip_reassignments_total{result}` | moves by result: `success`, `failure` or `dry_run` |
| `contabo_fip_api_errors_total{op}` | failed API calls by `op`: `get`, `assign` or `unassign` |
| `contabo_fip_reconcile_errors_total` | cycles that could not evaluate state, e.g. a misconfigured Service |

Example alerts. Because only the leader exports the state metrics, they
need no `contabo_fip_leader` join, and `absent()` catches a missing leader:

```yaml
- alert: ContaboFIPNoEligibleNode
  expr: contabo_fip_eligible_nodes == 0
  for: 1m
- alert: ContaboFIPNoLeader
  expr: absent(contabo_fip_last_reconcile_timestamp_seconds)
  for: 5m
- alert: ContaboFIPReconcileStale
  expr: time() - contabo_fip_last_reconcile_timestamp_seconds > 120
  for: 2m
- alert: ContaboFIPHolderNotEligible
  expr: |
    max(contabo_fip_holder) by (node)
      unless on (node) (contabo_fip_node_eligible == 1)
  for: 5m
- alert: ContaboFIPReassignFailing
  expr: increase(contabo_fip_reassignments_total{result="failure"}[15m]) > 0
```

## Development

```sh
go vet ./... && go test -race ./...
docker buildx build --platform linux/amd64 -t contabo-fip-controller:dev .
```

CI runs on the self-hosted runner scale set named by the repository variable
`PURE_VPS_RUNNER`, and falls back to `ubuntu-latest` while that variable is
unset. Pull requests always run on GitHub-hosted runners. Pushes to `main`
and `v*` tags publish `ghcr.io/korkin25/contabo-fip-controller`.

## License

Apache License 2.0, see [LICENSE](LICENSE).
