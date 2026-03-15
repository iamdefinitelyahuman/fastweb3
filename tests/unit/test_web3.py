# tests/unit/test_web3.py
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from fw3.errors import ValidationError
from fw3.provider import Provider
from fw3.web3 import Web3, Web3Config


@dataclass
class Call:
    method: str
    params: list[Any]
    route: str
    formatter: Any


class RecordingProvider(Provider):
    """
    Minimal Provider stand-in:
      - records every request call
      - returns pre-programmed raw results per method (or a default)
    """

    def __init__(self, *, results: Optional[Dict[str, Any]] = None, default: Any = None):
        # Don't call Provider.__init__ (we don't want endpoints)
        self._calls: List[Call] = []
        self._lock = threading.Lock()
        self._results = results or {}
        self._default = default
        self._called_event = threading.Event()
        self._primary = None

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        route: str = "pool",
        formatter=None,
        freshness=None,
    ) -> Any:
        # Web3.make_request should always pass formatter=None into provider
        with self._lock:
            self._calls.append(Call(method, list(params), route, formatter))
        self._called_event.set()

        if method in self._results:
            return self._results[method]
        return self._default

    def close(self) -> None:
        return

    @property
    def calls(self) -> List[Call]:
        with self._lock:
            return list(self._calls)

    def last_call(self) -> Call:
        with self._lock:
            assert self._calls, "No calls recorded"
            return self._calls[-1]


def force(x: Any) -> Any:
    _ = str(x)
    return x


def mk_w3(provider: RecordingProvider, *, strict: bool = True) -> Web3:
    return Web3(1, provider=provider, config=Web3Config(strict=strict))


def addr_bytes(b: int = 0x11) -> bytes:
    return bytes([b]) * 20


def h32_bytes(b: int = 0x22) -> bytes:
    return bytes([b]) * 32


@pytest.mark.parametrize(
    "fn_name, rpc_method, raw, expected, expected_route",
    [
        ("protocol_version", "eth_protocolVersion", "0x41", "0x41", "pool"),
        ("coinbase", "eth_coinbase", "0x" + "aa" * 20, "0x" + "aa" * 20, "primary"),
        ("mining", "eth_mining", True, True, "primary"),
        ("accounts", "eth_accounts", ["0x" + "bb" * 20], ["0x" + "bb" * 20], "primary"),
        ("chain_id", "eth_chainId", "0x1", 1, "pool"),
        ("hashrate", "eth_hashrate", "0x2a", 42, "primary"),
        ("gas_price", "eth_gasPrice", "0x3b9aca00", 1_000_000_000, "pool"),
        ("block_number", "eth_blockNumber", "0x10", 16, "pool"),
    ],
)
def test_eth_no_param_methods_wiring_and_formatting(
    fn_name, rpc_method, raw, expected, expected_route
):
    p = RecordingProvider(results={rpc_method: raw})
    w3 = mk_w3(p)

    out = force(getattr(w3.eth, fn_name)())
    assert out == expected

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == []
    assert c.route == expected_route
    assert c.formatter is None


def test_eth_syncing_normalizes_output_and_routes_primary():
    raw = {
        "startingBlock": "0x1",
        "currentBlock": "0x2",
        "highestBlock": "0x3",
        "pulledStates": "0x0",
        "knownStates": "0x0",
    }
    p = RecordingProvider(results={"eth_syncing": raw})
    w3 = mk_w3(p)

    out = force(w3.eth.syncing())
    assert isinstance(out, dict)
    assert out["startingBlock"] == "0x1"

    c = p.last_call()
    assert c.method == "eth_syncing"
    assert c.params == []
    assert c.route == "primary"


def test_get_balance_wiring_validation_and_to_int():
    a = addr_bytes(0x01)
    p = RecordingProvider(results={"eth_getBalance": "0x2a"})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_balance(a, "latest"))
    assert out == 42

    c = p.last_call()
    assert c.method == "eth_getBalance"
    assert c.params[0] == "0x" + a.hex()
    assert c.params[1] == "latest"
    assert c.route == "pool"


def test_get_balance_rejects_bad_address_in_strict_mode():
    p = RecordingProvider(results={"eth_getBalance": "0x0"})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError):
        force(w3.eth.get_balance("0x1234", "latest"))


def test_get_code_wiring():
    a = "0x" + "ab" * 20
    p = RecordingProvider(results={"eth_getCode": "0x6000"})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_code(a, 123))
    assert out == "0x6000"

    c = p.last_call()
    assert c.method == "eth_getCode"
    assert c.params == ["0x" + "ab" * 20, "0x7b"]
    assert c.route == "pool"


def test_get_storage_at_wiring():
    a = "0x" + "cd" * 20
    p = RecordingProvider(results={"eth_getStorageAt": "0x" + "00" * 32})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_storage_at(a, 5, "pending"))
    assert isinstance(out, str)

    c = p.last_call()
    assert c.method == "eth_getStorageAt"
    assert c.params == ["0x" + "cd" * 20, "0x5", "pending"]
    assert c.route == "pool"


def test_get_transaction_count_to_int_and_block_id():
    a = "0x" + "11" * 20
    p = RecordingProvider(results={"eth_getTransactionCount": "0x10"})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_transaction_count(a, 999))
    assert out == 16

    c = p.last_call()
    assert c.method == "eth_getTransactionCount"
    assert c.params == ["0x" + "11" * 20, "0x3e7"]
    assert c.route == "pool"


@pytest.mark.parametrize(
    "fn_name, rpc_method",
    [
        ("get_block_transaction_count_by_hash", "eth_getBlockTransactionCountByHash"),
        ("get_uncle_count_by_block_hash", "eth_getUncleCountByBlockHash"),
    ],
)
def test_hash_count_methods_wiring_and_to_int(fn_name, rpc_method):
    h = h32_bytes(0x22)
    p = RecordingProvider(results={rpc_method: "0x2"})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)(h))
    assert out == 2

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == ["0x" + h.hex()]
    assert c.route == "pool"


@pytest.mark.parametrize(
    "fn_name, rpc_method",
    [
        ("get_block_transaction_count_by_number", "eth_getBlockTransactionCountByNumber"),
        ("get_uncle_count_by_block_number", "eth_getUncleCountByBlockNumber"),
    ],
)
def test_block_number_count_methods_wiring_and_to_int(fn_name, rpc_method):
    p = RecordingProvider(results={rpc_method: "0x7"})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)("finalized"))
    assert out == 7

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == ["finalized"]
    assert c.route == "pool"


@pytest.mark.parametrize(
    "fn_name, rpc_method",
    [
        ("get_transaction_by_hash", "eth_getTransactionByHash"),
        ("get_transaction_receipt", "eth_getTransactionReceipt"),
    ],
)
def test_tx_hash_methods_normalize_output(fn_name, rpc_method):
    txh = "0x" + "aa" * 32
    raw = {"blockNumber": "0x10", "transactionIndex": "0x2", "hash": txh}
    p = RecordingProvider(results={rpc_method: raw})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)(txh))
    assert out["blockNumber"] == 16
    assert out["transactionIndex"] == 2
    assert out["hash"] == txh

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == [txh]
    assert c.route == "pool"


@pytest.mark.parametrize(
    "fn_name, rpc_method, params_builder",
    [
        (
            "get_transaction_by_block_hash_and_index",
            "eth_getTransactionByBlockHashAndIndex",
            lambda h: [h, "0x2"],
        ),
        (
            "get_uncle_by_block_hash_and_index",
            "eth_getUncleByBlockHashAndIndex",
            lambda h: [h, "0x2"],
        ),
    ],
)
def test_block_hash_and_index_methods(fn_name, rpc_method, params_builder):
    bh = "0x" + "11" * 32
    raw = {"number": "0x1", "transactionIndex": "0x0"}
    p = RecordingProvider(results={rpc_method: raw})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)(bh, 2))
    assert out["number"] == 1
    assert out["transactionIndex"] == 0

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == params_builder(bh)
    assert c.route == "pool"


@pytest.mark.parametrize(
    "fn_name, rpc_method, params_builder",
    [
        (
            "get_transaction_by_block_number_and_index",
            "eth_getTransactionByBlockNumberAndIndex",
            lambda blk: [blk, "0x1"],
        ),
        (
            "get_uncle_by_block_number_and_index",
            "eth_getUncleByBlockNumberAndIndex",
            lambda blk: [blk, "0x1"],
        ),
    ],
)
def test_block_number_and_index_methods(fn_name, rpc_method, params_builder):
    raw = {"number": "0x2"}
    p = RecordingProvider(results={rpc_method: raw})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)(123, 1))
    assert out["number"] == 2

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == params_builder("0x7b")
    assert c.route == "pool"


def test_get_block_by_hash_wiring_and_normalize():
    bh = "0x" + "33" * 32
    raw = {"number": "0x10", "timestamp": "0x5", "transactions": []}
    p = RecordingProvider(results={"eth_getBlockByHash": raw})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_block_by_hash(bh, full_transactions=False))
    assert out["number"] == 16
    assert out["timestamp"] == 5

    c = p.last_call()
    assert c.method == "eth_getBlockByHash"
    assert c.params == [bh, False]
    assert c.route == "pool"


def test_get_block_by_number_wiring_and_normalize():
    raw = {"number": "0x2", "size": "0x10"}
    p = RecordingProvider(results={"eth_getBlockByNumber": raw})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_block_by_number("latest", full_transactions=True))
    assert out["number"] == 2
    assert out["size"] == 16

    c = p.last_call()
    assert c.method == "eth_getBlockByNumber"
    assert c.params == ["latest", True]
    assert c.route == "pool"


def test_sign_routes_primary_and_validates_data_hex():
    a = "0x" + "12" * 20
    p = RecordingProvider(results={"eth_sign": "0x" + "99" * 65})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.sign(a, b"\x01\x02"))
    assert isinstance(out, str)

    c = p.last_call()
    assert c.method == "eth_sign"
    assert c.route == "primary"
    assert c.params == [a, "0x0102"]


def test_send_raw_transaction_disallows_empty():
    p = RecordingProvider(results={"eth_sendRawTransaction": "0x" + "aa" * 32})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError):
        force(w3.eth.send_raw_transaction(b""))


def test_send_raw_transaction_primary_vs_pool():
    p = RecordingProvider(results={"eth_sendRawTransaction": "0x" + "aa" * 32})
    w3 = mk_w3(p, strict=True)

    # with primary set, call should be routed to primary
    p._primary = True
    w3.eth.send_raw_transaction(b"\x01\x02")
    c = p.last_call()
    assert c.method == "eth_sendRawTransaction"
    assert c.route == "primary"

    # when unset, should be routed to pool
    p._primary = None
    w3.eth.send_raw_transaction(b"\x01\x02")
    c = p.last_call()
    assert c.method == "eth_sendRawTransaction"
    assert c.route == "pool"


def test_sign_transaction_requires_from_and_to_or_data_in_strict():
    p = RecordingProvider(results={"eth_signTransaction": {"raw": "0xdead"}})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError, match="requires 'from'"):
        force(w3.eth.sign_transaction(to="0x" + "11" * 20))

    with pytest.raises(ValidationError, match="requires at least one of 'to' or 'data'"):
        force(w3.eth.sign_transaction(from_="0x" + "11" * 20))


def test_send_transaction_requires_from_and_to_or_data_in_strict():
    p = RecordingProvider(results={"eth_sendTransaction": "0x" + "aa" * 32})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError, match="requires 'from'"):
        force(w3.eth.send_transaction(to="0x" + "11" * 20))

    with pytest.raises(ValidationError, match="requires at least one of 'to' or 'data'"):
        force(w3.eth.send_transaction(from_="0x" + "11" * 20))


def test_tx_object_fee_field_invariants_enforced():
    p = RecordingProvider(results={"eth_sendTransaction": "0x" + "aa" * 32})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError, match="cannot specify gasPrice"):
        force(
            w3.eth.send_transaction(
                from_="0x" + "11" * 20,
                to="0x" + "22" * 20,
                gas_price=1,
                max_fee_per_gas=2,
                max_priority_fee_per_gas=1,
            )
        )

    with pytest.raises(ValidationError, match="must specify both maxFeePerGas"):
        force(
            w3.eth.send_transaction(
                from_="0x" + "11" * 20,
                to="0x" + "22" * 20,
                max_fee_per_gas=2,
            )
        )


def test_send_transaction_wiring_and_routes_primary():
    p = RecordingProvider(results={"eth_sendTransaction": "0x" + "aa" * 32})
    w3 = mk_w3(p, strict=True)

    out = force(
        w3.eth.send_transaction(
            from_=addr_bytes(0x01),
            to=addr_bytes(0x02),
            value=5,
            data=b"\x12\x34",
        )
    )
    assert isinstance(out, str)

    c = p.last_call()
    assert c.method == "eth_sendTransaction"
    assert c.route == "primary"

    tx = c.params[0]
    assert isinstance(tx, dict)
    assert tx["from"] == "0x" + addr_bytes(0x01).hex()
    assert tx["to"] == "0x" + addr_bytes(0x02).hex()
    assert tx["value"] == "0x5"
    assert tx["data"] == "0x1234"


def test_call_requires_to_or_data_in_strict():
    p = RecordingProvider(results={"eth_call": "0x"})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError, match="eth_call requires at least one of 'to' or 'data'"):
        force(w3.eth.call(block="latest"))


def test_call_wiring():
    p = RecordingProvider(results={"eth_call": "0x1234"})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.call(to=addr_bytes(0x02), data="0x", block=100))
    assert out == "0x1234"

    c = p.last_call()
    assert c.method == "eth_call"
    assert c.route == "pool"
    assert isinstance(c.params[0], dict)
    assert c.params[1] == "0x64"


def test_estimate_gas_requires_to_or_data_in_strict():
    p = RecordingProvider(results={"eth_estimateGas": "0x5208"})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(
        ValidationError, match="eth_estimateGas requires at least one of 'to' or 'data'"
    ):
        force(w3.eth.estimate_gas())


def test_estimate_gas_wiring_and_optional_block_and_to_int():
    p = RecordingProvider(results={"eth_estimateGas": "0x5208"})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.estimate_gas(to=addr_bytes(0x02), block="pending"))
    assert out == 0x5208

    c = p.last_call()
    assert c.method == "eth_estimateGas"
    assert c.route == "pool"
    assert c.params[0]["to"] == "0x" + addr_bytes(0x02).hex()
    assert c.params[1] == "pending"


def test_new_filter_builds_filter_object_and_routes_primary():
    p = RecordingProvider(results={"eth_newFilter": "0x1"})
    w3 = mk_w3(p, strict=True)

    out = force(
        w3.eth.new_filter(
            from_block=10,
            to_block="latest",
            address=[addr_bytes(0x01), "0x" + "22" * 20],
            topics=[None, h32_bytes(0x33), [h32_bytes(0x44), "0x" + "55" * 32]],
        )
    )
    assert out == "0x1"

    c = p.last_call()
    assert c.method == "eth_newFilter"
    assert c.route == "primary"

    flt = c.params[0]
    assert flt["fromBlock"] == "0xa"
    assert flt["toBlock"] == "latest"
    assert flt["address"][0] == "0x" + addr_bytes(0x01).hex()
    assert flt["topics"][0] is None
    assert flt["topics"][1] == "0x" + h32_bytes(0x33).hex()
    assert flt["topics"][2][0] == "0x" + h32_bytes(0x44).hex()
    assert flt["topics"][2][1] == "0x" + ("55" * 32)


def test_filter_object_invariants_blockhash_cannot_mix_with_from_to():
    p = RecordingProvider(results={"eth_newFilter": "0x1"})
    w3 = mk_w3(p, strict=True)

    with pytest.raises(ValidationError, match="cannot combine blockHash"):
        force(w3.eth.new_filter(block_hash=h32_bytes(0x11), from_block=1))


@pytest.mark.parametrize(
    "fn_name, rpc_method, raw",
    [
        ("new_block_filter", "eth_newBlockFilter", "0x1"),
        ("new_pending_transaction_filter", "eth_newPendingTransactionFilter", "0x2"),
    ],
)
def test_simple_filter_creators_wiring_and_route_primary(fn_name, rpc_method, raw):
    p = RecordingProvider(results={rpc_method: raw})
    w3 = mk_w3(p, strict=True)

    out = force(getattr(w3.eth, fn_name)())
    assert out == raw

    c = p.last_call()
    assert c.method == rpc_method
    assert c.params == []
    assert c.route == "primary"


def test_uninstall_filter_wiring_and_route_primary():
    p = RecordingProvider(results={"eth_uninstallFilter": True})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.uninstall_filter("0x1"))
    assert bool(out) is True

    c = p.last_call()
    assert c.method == "eth_uninstallFilter"
    assert c.params == ["0x1"]
    assert c.route == "primary"


def test_get_filter_changes_and_logs_normalize_and_route_primary():
    raw_changes = [{"blockNumber": "0x1", "logIndex": "0x0"}]
    raw_logs = [{"blockNumber": "0x2", "logIndex": "0x1"}]
    p = RecordingProvider(
        results={"eth_getFilterChanges": raw_changes, "eth_getFilterLogs": raw_logs}
    )
    w3 = mk_w3(p, strict=True)

    out1 = force(w3.eth.get_filter_changes("0x1"))
    assert out1[0]["blockNumber"] == 1
    assert out1[0]["logIndex"] == 0
    c1 = p.last_call()
    assert c1.method == "eth_getFilterChanges"
    assert c1.route == "primary"

    out2 = force(w3.eth.get_filter_logs("0x1"))
    assert out2[0]["blockNumber"] == 2
    assert out2[0]["logIndex"] == 1
    c2 = p.last_call()
    assert c2.method == "eth_getFilterLogs"
    assert c2.route == "primary"


def test_get_logs_builds_filter_object_and_normalizes_pool_route():
    raw = [{"blockNumber": "0x10", "logIndex": "0x2"}]
    p = RecordingProvider(results={"eth_getLogs": raw})
    w3 = mk_w3(p, strict=True)

    out = force(w3.eth.get_logs(from_block="earliest", to_block=5))
    assert out[0]["blockNumber"] == 16
    assert out[0]["logIndex"] == 2

    c = p.last_call()
    assert c.method == "eth_getLogs"
    assert c.route == "pool"
    flt = c.params[0]
    assert flt["fromBlock"] == "earliest"
    assert flt["toBlock"] == "0x5"


def test_non_strict_does_not_lowercase_or_validate_str_address():
    a = " 0xAbCd "  # invalid length, but non-strict should allow
    p = RecordingProvider(results={"eth_getBalance": "0x1"})
    w3 = mk_w3(p, strict=False)

    out = force(w3.eth.get_balance(a, " latest "))
    assert out == 1

    c = p.last_call()
    assert c.params[0] == "0xAbCd"
    assert c.params[1] == "latest"
    assert c.route == "pool"


def test_block_ref_methods_accept_block_hash_in_strict_mode():
    bh = h32_bytes(0x77)
    p = RecordingProvider(
        results={
            "eth_getBalance": "0x2a",
            "eth_getCode": "0x6000",
            "eth_getStorageAt": "0x" + "00" * 32,
            "eth_getTransactionCount": "0x10",
            "eth_call": "0x1234",
            "eth_getProof": {
                "address": "0x" + "11" * 20,
                "balance": "0x0",
                "storageHash": "0x" + "22" * 32,
                "codeHash": "0x" + "33" * 32,
                "accountProof": [],
                "storageProof": [],
            },
        }
    )
    w3 = mk_w3(p, strict=True)

    assert force(w3.eth.get_balance(addr_bytes(0x01), bh)) == 42
    assert p.last_call().params == ["0x" + addr_bytes(0x01).hex(), "0x" + bh.hex()]

    assert force(w3.eth.get_code("0x" + "ab" * 20, bh)) == "0x6000"
    assert p.last_call().params == ["0x" + "ab" * 20, "0x" + bh.hex()]

    force(w3.eth.get_storage_at("0x" + "cd" * 20, 5, bh))
    assert p.last_call().params == ["0x" + "cd" * 20, "0x5", "0x" + bh.hex()]

    assert force(w3.eth.get_transaction_count("0x" + "11" * 20, bh)) == 16
    assert p.last_call().params == ["0x" + "11" * 20, "0x" + bh.hex()]

    assert force(w3.eth.call(to=addr_bytes(0x02), data="0x", block=bh)) == "0x1234"
    assert p.last_call().params[1] == "0x" + bh.hex()

    proof = force(w3.eth.get_proof("0x" + "11" * 20, [b"", b"\x01\x02"], bh))
    assert proof["address"] == "0x" + "11" * 20
    assert p.last_call().params == ["0x" + "11" * 20, ["0x", "0x0102"], "0x" + bh.hex()]


def test_eth_new_spec_methods_wiring_and_validation():
    bh = h32_bytes(0x66)
    p = RecordingProvider(
        results={
            "eth_blobBaseFee": "0x3",
            "eth_createAccessList": {"accessList": [], "gasUsed": "0x5208"},
            "eth_feeHistory": {
                "oldestBlock": "0x10",
                "baseFeePerGas": ["0x1", "0x2"],
                "gasUsedRatio": [0.5],
                "reward": [["0x3"]],
            },
            "eth_getBlockAccessList": [{"address": "0x" + "11" * 20, "storageKeys": []}],
            "eth_getBlockReceipts": [{"blockNumber": "0x10", "transactionIndex": "0x0"}],
            "eth_getProof": {
                "address": "0x" + "11" * 20,
                "balance": "0x0",
                "storageHash": "0x" + "22" * 32,
                "codeHash": "0x" + "33" * 32,
                "accountProof": [],
                "storageProof": [],
            },
            "eth_maxPriorityFeePerGas": "0x4",
            "eth_simulateV1": [{"calls": []}],
        }
    )
    w3 = mk_w3(p, strict=True)

    assert force(w3.eth.blob_base_fee()) == 3
    assert p.last_call().method == "eth_blobBaseFee"
    assert p.last_call().params == []

    out = force(w3.eth.create_access_list(to=addr_bytes(0x02), block="pending"))
    assert out["gasUsed"] == 0x5208
    c = p.last_call()
    assert c.method == "eth_createAccessList"
    assert c.params[0]["to"] == "0x" + addr_bytes(0x02).hex()
    assert c.params[1] == "pending"

    with pytest.raises(
        ValidationError, match="eth_createAccessList requires at least one of 'to' or 'data'"
    ):
        force(w3.eth.create_access_list())

    out = force(w3.eth.fee_history(2, "latest", [10.0, 50.0]))
    assert out["oldestBlock"] == 16
    assert out["baseFeePerGas"] == [1, 2]
    assert out["reward"] == [[3]]
    c = p.last_call()
    assert c.method == "eth_feeHistory"
    assert c.params == ["0x2", "latest", [10.0, 50.0]]

    with pytest.raises(ValidationError, match="between 0 and 100"):
        force(w3.eth.fee_history(2, "latest", [101.0]))

    with pytest.raises(ValidationError, match="monotonically increasing"):
        force(w3.eth.fee_history(2, "latest", [50.0, 10.0]))

    out = force(w3.eth.get_block_access_list(bh))
    assert out[0]["address"] == "0x" + "11" * 20
    c = p.last_call()
    assert c.method == "eth_getBlockAccessList"
    assert c.params == ["0x" + bh.hex()]

    out = force(w3.eth.get_block_receipts(bh))
    assert out[0]["blockNumber"] == 16
    assert out[0]["transactionIndex"] == 0
    c = p.last_call()
    assert c.method == "eth_getBlockReceipts"
    assert c.params == ["0x" + bh.hex()]

    out = force(w3.eth.get_proof("0x" + "11" * 20, [b"", b"\x01\x02"], bh))
    assert out["address"] == "0x" + "11" * 20
    c = p.last_call()
    assert c.method == "eth_getProof"
    assert c.params == ["0x" + "11" * 20, ["0x", "0x0102"], "0x" + bh.hex()]

    assert force(w3.eth.max_priority_fee_per_gas()) == 4
    assert p.last_call().method == "eth_maxPriorityFeePerGas"
    assert p.last_call().params == []

    out = force(w3.eth.simulate_v1({"blockStateCalls": []}, bh))
    assert out[0]["calls"] == []
    c = p.last_call()
    assert c.method == "eth_simulateV1"
    assert c.params == [{"blockStateCalls": []}, "0x" + bh.hex()]

    with pytest.raises(ValidationError, match="eth_simulateV1 requires 'payload' to be a mapping"):
        force(w3.eth.simulate_v1(["nope"]))  # type: ignore[arg-type]
