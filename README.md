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
  - If the API refuses because the IP is assigned elsewhere, the controller
    unassigns the old holder and assigns again.
  - Every move is checked by reading the VIP back. Failures back off
    exponentially, up to `--max-backoff`.
- **Never unassigns** when no node is eligible. Pulling the IP from a node
  that might still serve some traffic does not help anyone.
- The holder is cached and re-read from the API every `--vip-refresh` (30s).
- Nodes are skipped when they:
  - lack the `contabo.com/instance-id` label;
  - carry `node.kubernetes.io/exclude-from-external-load-balancers`;
  - are being deleted.
- Two replicas run with Lease leader election, and only the leader acts.
- On every move the controller records Events on the watched Services.

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
| `--max-backoff` | `1m` | cap on retry delay |
| `--dry-run` | `false` | decide and emit Events, never write to the API |
| `--leader-elect` | `true` | Lease `--lease-name` in `--lease-namespace` (`POD_NAMESPACE`) |
| `--metrics-addr` | `:8080` | `/metrics`, `/healthz` |

Deploy with `kubectl apply -k deploy/example` after you edit the overlay.

## Metrics

| Metric | Meaning |
|---|---|
| `contabo_fip_leader` | `1` on the replica that holds the Lease |
| `contabo_fip_node_eligible{node}` | per-node verdict |
| `contabo_fip_eligible_nodes` | number of eligible nodes; alert when it is `0` |
| `contabo_fip_holder{node,instance_id}` | the current holder; `node` is empty when the holder is not a known node |
| `contabo_fip_reassignments_total{result}` | moves by result: `success`, `failure` or `dry_run` |
| `contabo_fip_api_errors_total{op}` | failed API calls by `op`: `get`, `assign` or `unassign` |
| `contabo_fip_reconcile_errors_total` | cycles that could not evaluate state, e.g. a misconfigured Service |

## Development

```sh
go vet ./... && go test -race ./...
docker buildx build --platform linux/amd64 -t contabo-fip-controller:dev .
```

CI runs on the self-hosted runner scale set named by the repository variable
`PURE_VPS_RUNNER`, and falls back to `ubuntu-latest` while that variable is
unset. Pull requests always run on GitHub-hosted runners. Pushes to `main`
and `v*` tags publish `ghcr.io/korkin25/contabo-fip-controller`.
