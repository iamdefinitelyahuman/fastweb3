# fastweb3

High-performance Web3 client.

**NOTE**: This library is still in early alpha development. Prior to a `v1.0.0` (which may never come), expect breaking changes and no backward compatibility between versions.

## Installation

You can install the latest release via `pip`:

```bash
pip install fastweb3
```

Or clone the repository for the most up-to-date version:

```bash
git clone https://github.com/iamdefinitelyahuman/fastweb3.git
cd fastweb3
pip install -e .
```

## Features

### Automatic public RPC discovery and pooling

`fastweb3` queries a [list of public RPC endpoints](https://github.com/ethereum-lists/chains), maintains a pool of useable nodes, distributes requests between them, and reroutes failed requests. Users can rely on public infrastructure without thinking about timeouts, rate limiting, nodes falling out of sync, etc.

```py
>>> from fastweb3 import Web3

# only a chainId is needed to connect
>>> w3 = Web3(1)

# the object immediately begins querying endpoints to check availability and latency
# the best nodes are selected to be used in an "active pool"
>>> w3.active_pool_size()
6

# we continue to monitor all known good endpoints, in case we have issues with any
# member of the active pool
>>> w3.pool_capacity()
11
```

Users with their own RPC can target it as their primary endpoint. This endpoint is then favored for write methods, but read methods continue to be distributed amongst the node pool.

```py
>>> w3 = Web3(1, primary_endpoint=["my.local.node"])
```

### Deferred Execution

RPC calls return [`Proxy`](github.com/ionelmc/python-lazy-object-proxy) objects immediately, while network I/O happens in the background.

```py
>>> amount = w3.eth.get_balance('0xd8da6bf26964af9d7eed9e03e53415d37aa96045')
>>> amount
<Proxy at 0x7b9ac0764a40 of object ... >
>>> print(amount)
32131215082101779377
```

### Simple Batching Semantics

Batching uses natural syntax. With a context manager open, each requests is queued in the same batch until one of the `Proxy` objects is read, at which point the entire request is processed. This also guarantees queried values are all read from the same block.

```py
>>> with w3.batch_requests():
...     a = w3.eth.get_balance("0xd8da6bf26964af9d7eed9e03e53415d37aa96045")
...     b = w3.eth.get_balance("0x1db3439a222c519ab44bb1144fc28167b4fa6ee6")
...     # both eth_getBalance queries are sent in the same batched request
...     print(a + b)
...
32840401623804415458
```

## Tests

First, install the dev dependencies:

```bash
pip install -e ".[dev]"
```

To run the test suite:

```bash
pytest
```

## License

This project is licensed under the [MIT license](LICENSE).
