from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

from .deferred import Handle, deferred_response
from .errors import NoEndpoints
from .formatters import to_int
from .provider import Provider, RetryPolicy

BlockId = Union[
    str, int
]  # "latest" | "pending" | "earliest" | "safe" | "finalized" | hex str | int


@dataclass(frozen=True)
class Web3Config:
    retry_policy_read: RetryPolicy = RetryPolicy(max_attempts=3, backoff_seconds=0.05)
    retry_policy_write: RetryPolicy = RetryPolicy(max_attempts=1)
    # later: batching/hedging/quorum knobs


class Web3:
    """
    Chain-id-centric Web3 entrypoint.

    v0 usage:
        w3 = Web3(1, endpoints=[...])
        w3.eth.block_number()
    """

    def __init__(
        self,
        chain_id: int,
        *,
        endpoints: Optional[Sequence[str]] = None,
        provider: Optional[Provider] = None,
        config: Optional[Web3Config] = None,
    ) -> None:
        self.chain_id = int(chain_id)
        self.config = config or Web3Config()

        if provider is not None:
            self.provider = provider
        else:
            urls = list(endpoints or [])
            if not urls:
                raise NoEndpoints(
                    f"No endpoints for chain_id={self.chain_id}. "
                    "Provide endpoints=[...]. (Discovery/chainlist comes later.)"
                )
            self.provider = Provider(
                urls,
                retry_policy_read=self.config.retry_policy_read,
                retry_policy_write=self.config.retry_policy_write,
            )

        # Namespaces (web3.py-style)
        self.eth = Eth(self)

    def close(self) -> None:
        self.provider.close()

    def make_request(
        self, method: str, params: list[Any], *, kind: str = "read", formatter=None
    ) -> Any:
        """
        Perform a raw JSON-RPC request.

        method: e.g. "eth_getBalance"
        params: JSON-RPC params list
        kind: "read" or "write" (routing hint)
        formatter: optional post-processor for result
        """

        def bg_func(h: Handle) -> None:
            # No formatter here: formatting must only happen in format_func.
            raw = self.provider.request(method, params, kind=kind, formatter=None)
            h.set_value(raw)

        # ref_func unused for now (later: batching flush barrier)
        return deferred_response(bg_func, format_func=formatter, ref_func=None)

    # ---- scaffolding for later batching ----

    def pin(self, *, kind: str = "read"):
        return self.provider.pin(kind=kind)

    def batch(self):
        raise NotImplementedError("Batching not implemented yet")


class Eth:
    """
    Core eth_* JSON-RPC namespace as listed on ethereum.org.

    Notes:
      - For "transaction object" params, methods take keyword-only args and build the dict.
      - For "filter object" params, methods take keyword-only args and build the dict.
      - Quantity inputs may be passed as int (we hex-encode) or as already-encoded 0x strings.
    """

    def __init__(self, w3: "Web3") -> None:
        self._w3 = w3

    # ----------------------------
    # helpers
    # ----------------------------

    @staticmethod
    def _q(x: int | str | None) -> str | None:
        """Quantity: int -> 0x hex. Pass through 0x strings. None stays None."""
        if x is None:
            return None
        if isinstance(x, int):
            if x < 0:
                raise ValueError("Quantity ints must be >= 0")
            return hex(x)
        if isinstance(x, str):
            return x
        raise TypeError(f"Expected int|str|None, got {type(x).__name__}")

    @staticmethod
    def _block(x: BlockId) -> str:
        """Block parameter: int -> 0x hex, else pass through (e.g. 'latest')."""
        if isinstance(x, int):
            if x < 0:
                raise ValueError("Block number must be >= 0")
            return hex(x)
        if isinstance(x, str):
            return x
        raise TypeError(f"Expected str|int for block id, got {type(x).__name__}")

    @staticmethod
    def _index(x: int | str) -> str:
        """Index parameter: int -> 0x hex, else pass through (0x...)."""
        if isinstance(x, int):
            if x < 0:
                raise ValueError("Index must be >= 0")
            return hex(x)
        if isinstance(x, str):
            return x
        raise TypeError(f"Expected str|int for index, got {type(x).__name__}")

    def _tx_object(
        self,
        *,
        from_: str | None = None,
        to: str | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        tx: dict[str, Any] = {}

        if from_ is not None:
            tx["from"] = from_
        if to is not None:
            tx["to"] = to
        if gas is not None:
            tx["gas"] = self._q(gas)
        if gas_price is not None:
            tx["gasPrice"] = self._q(gas_price)
        if max_fee_per_gas is not None:
            tx["maxFeePerGas"] = self._q(max_fee_per_gas)
        if max_priority_fee_per_gas is not None:
            tx["maxPriorityFeePerGas"] = self._q(max_priority_fee_per_gas)
        if value is not None:
            tx["value"] = self._q(value)
        if data is not None:
            tx["data"] = data
        if nonce is not None:
            tx["nonce"] = self._q(nonce)
        if chain_id is not None:
            tx["chainId"] = self._q(chain_id)
        if type_ is not None:
            tx["type"] = self._q(type_)
        if access_list is not None:
            tx["accessList"] = access_list

        return tx

    def _filter_object(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | list[str] | None = None,
        topics: list[str | list[str] | None] | None = None,
        block_hash: str | None = None,
    ) -> dict[str, Any]:
        flt: dict[str, Any] = {}

        # Spec: blockHash cannot be used with fromBlock/toBlock on most clients.
        if block_hash is not None:
            flt["blockHash"] = block_hash

        if from_block is not None:
            flt["fromBlock"] = self._block(from_block)
        if to_block is not None:
            flt["toBlock"] = self._block(to_block)
        if address is not None:
            flt["address"] = address
        if topics is not None:
            flt["topics"] = topics

        return flt

    # ----------------------------
    # methods (ethereum.org list)
    # ----------------------------

    def protocol_version(self) -> str:
        return self._w3.make_request("eth_protocolVersion", [])

    def syncing(self) -> bool | dict[str, Any]:
        return self._w3.make_request("eth_syncing", [])

    def coinbase(self) -> str:
        return self._w3.make_request("eth_coinbase", [])

    def chain_id(self) -> int:
        return self._w3.make_request("eth_chainId", [], formatter=to_int)

    def mining(self) -> bool:
        return self._w3.make_request("eth_mining", [])

    def hashrate(self) -> int:
        return self._w3.make_request("eth_hashrate", [], formatter=to_int)

    def gas_price(self) -> int:
        return self._w3.make_request("eth_gasPrice", [], formatter=to_int)

    def accounts(self) -> list[str]:
        return self._w3.make_request("eth_accounts", [])

    def block_number(self) -> int:
        return self._w3.make_request("eth_blockNumber", [], formatter=to_int)

    def get_balance(self, address: str, block: BlockId = "latest") -> int:
        return self._w3.make_request(
            "eth_getBalance", [address, self._block(block)], formatter=to_int
        )

    def get_storage_at(self, address: str, position: int | str, block: BlockId = "latest") -> str:
        return self._w3.make_request(
            "eth_getStorageAt",
            [address, self._q(position), self._block(block)],
        )

    def get_transaction_count(self, address: str, block: BlockId = "latest") -> int:
        return self._w3.make_request(
            "eth_getTransactionCount",
            [address, self._block(block)],
            formatter=to_int,
        )

    def get_block_transaction_count_by_hash(self, block_hash: str) -> int:
        return self._w3.make_request(
            "eth_getBlockTransactionCountByHash", [block_hash], formatter=to_int
        )

    def get_block_transaction_count_by_number(self, block: BlockId) -> int:
        return self._w3.make_request(
            "eth_getBlockTransactionCountByNumber",
            [self._block(block)],
            formatter=to_int,
        )

    def get_uncle_count_by_block_hash(self, block_hash: str) -> int:
        return self._w3.make_request("eth_getUncleCountByBlockHash", [block_hash], formatter=to_int)

    def get_uncle_count_by_block_number(self, block: BlockId) -> int:
        return self._w3.make_request(
            "eth_getUncleCountByBlockNumber", [self._block(block)], formatter=to_int
        )

    def get_code(self, address: str, block: BlockId = "latest") -> str:
        return self._w3.make_request("eth_getCode", [address, self._block(block)])

    def sign(self, address: str, data: str) -> str:
        # "write" routing hint: uses node-local account
        return self._w3.make_request("eth_sign", [address, data], kind="write")

    def sign_transaction(
        self,
        *,
        from_: str | None = None,
        to: str | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        tx = self._tx_object(
            from_=from_,
            to=to,
            gas=gas,
            gas_price=gas_price,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            value=value,
            data=data,
            nonce=nonce,
            chain_id=chain_id,
            type_=type_,
            access_list=access_list,
        )
        return self._w3.make_request("eth_signTransaction", [tx], kind="write")

    def send_transaction(
        self,
        *,
        from_: str | None = None,
        to: str | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> str:
        tx = self._tx_object(
            from_=from_,
            to=to,
            gas=gas,
            gas_price=gas_price,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            value=value,
            data=data,
            nonce=nonce,
            chain_id=chain_id,
            type_=type_,
            access_list=access_list,
        )
        return self._w3.make_request("eth_sendTransaction", [tx], kind="write")

    def send_raw_transaction(self, signed_tx: str) -> str:
        return self._w3.make_request("eth_sendRawTransaction", [signed_tx], kind="write")

    def call(
        self,
        *,
        to: str | None = None,
        from_: str | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
        block: BlockId = "latest",
    ) -> str:
        tx = self._tx_object(
            from_=from_,
            to=to,
            gas=gas,
            gas_price=gas_price,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            value=value,
            data=data,
            nonce=nonce,
            chain_id=chain_id,
            type_=type_,
            access_list=access_list,
        )
        return self._w3.make_request("eth_call", [tx, self._block(block)])

    def estimate_gas(
        self,
        *,
        to: str | None = None,
        from_: str | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
        block: BlockId | None = None,
    ) -> int:
        tx = self._tx_object(
            from_=from_,
            to=to,
            gas=gas,
            gas_price=gas_price,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            value=value,
            data=data,
            nonce=nonce,
            chain_id=chain_id,
            type_=type_,
            access_list=access_list,
        )
        params: list[Any] = [tx]
        if block is not None:
            params.append(self._block(block))
        return self._w3.make_request("eth_estimateGas", params, formatter=to_int)

    def get_block_by_hash(
        self, block_hash: str, full_transactions: bool = False
    ) -> dict[str, Any] | None:
        return self._w3.make_request("eth_getBlockByHash", [block_hash, full_transactions])

    def get_block_by_number(
        self, block: BlockId, full_transactions: bool = False
    ) -> dict[str, Any] | None:
        return self._w3.make_request(
            "eth_getBlockByNumber", [self._block(block), full_transactions]
        )

    def get_transaction_by_hash(self, tx_hash: str) -> dict[str, Any] | None:
        return self._w3.make_request("eth_getTransactionByHash", [tx_hash])

    def get_transaction_by_block_hash_and_index(
        self, block_hash: str, index: int | str
    ) -> dict[str, Any] | None:
        return self._w3.make_request(
            "eth_getTransactionByBlockHashAndIndex", [block_hash, self._index(index)]
        )

    def get_transaction_by_block_number_and_index(
        self, block: BlockId, index: int | str
    ) -> dict[str, Any] | None:
        return self._w3.make_request(
            "eth_getTransactionByBlockNumberAndIndex",
            [self._block(block), self._index(index)],
        )

    def get_transaction_receipt(self, tx_hash: str) -> dict[str, Any] | None:
        return self._w3.make_request("eth_getTransactionReceipt", [tx_hash])

    def get_uncle_by_block_hash_and_index(
        self, block_hash: str, index: int | str
    ) -> dict[str, Any] | None:
        return self._w3.make_request(
            "eth_getUncleByBlockHashAndIndex", [block_hash, self._index(index)]
        )

    def get_uncle_by_block_number_and_index(
        self, block: BlockId, index: int | str
    ) -> dict[str, Any] | None:
        return self._w3.make_request(
            "eth_getUncleByBlockNumberAndIndex", [self._block(block), self._index(index)]
        )

    def new_filter(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | list[str] | None = None,
        topics: list[str | list[str] | None] | None = None,
        block_hash: str | None = None,
    ) -> str:
        flt = self._filter_object(
            from_block=from_block,
            to_block=to_block,
            address=address,
            topics=topics,
            block_hash=block_hash,
        )
        return self._w3.make_request("eth_newFilter", [flt])

    def new_block_filter(self) -> str:
        return self._w3.make_request("eth_newBlockFilter", [])

    def new_pending_transaction_filter(self) -> str:
        return self._w3.make_request("eth_newPendingTransactionFilter", [])

    def uninstall_filter(self, filter_id: str) -> bool:
        return self._w3.make_request("eth_uninstallFilter", [filter_id])

    def get_filter_changes(self, filter_id: str) -> list[Any]:
        return self._w3.make_request("eth_getFilterChanges", [filter_id])

    def get_filter_logs(self, filter_id: str) -> list[Any]:
        return self._w3.make_request("eth_getFilterLogs", [filter_id])

    def get_logs(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | list[str] | None = None,
        topics: list[str | list[str] | None] | None = None,
        block_hash: str | None = None,
    ) -> list[Any]:
        flt = self._filter_object(
            from_block=from_block,
            to_block=to_block,
            address=address,
            topics=topics,
            block_hash=block_hash,
        )
        return self._w3.make_request("eth_getLogs", [flt])
