from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

from . import validation
from .chains import provider_for_chain
from .deferred import Handle, deferred_response
from .errors import NoEndpoints, ValidationError
from .formatters import normalize_rpc_obj, to_int
from .provider import Provider, RetryPolicy

BlockId = Union[
    str, int
]  # "latest" | "pending" | "earliest" | "safe" | "finalized" | hex str | int


@dataclass(frozen=True)
class Web3Config:
    strict: bool = True
    retry_policy_read: RetryPolicy = RetryPolicy(max_attempts=3, backoff_seconds=0.05)
    retry_policy_write: RetryPolicy = RetryPolicy(max_attempts=1)
    # later: batching/hedging/quorum knobs
    # later: output formatting mode knobs (raw vs normalized)


class Web3:
    """
    Web3 entrypoint.

    Usage:
        w3 = Web3(1)                          # auto-discover public endpoints for chain 1
        w3 = Web3(endpoints=[...])            # manual endpoints only (no discovery)
        w3 = Web3(1, endpoints=[...])         # hybrid: user endpoints (primary) + public fallbacks
        w3 = Web3(provider=my_provider)       # fully custom provider (advanced)
    """

    def __init__(
        self,
        chain_id: Optional[int] = None,
        *,
        endpoints: Optional[Sequence[str]] = None,
        provider: Optional[Provider] = None,
        config: Optional[Web3Config] = None,
    ) -> None:
        self.config = config or Web3Config()

        # Chain id is metadata only in discovery/hybrid mode.
        self._chain_id = int(chain_id) if chain_id is not None else None

        if provider is not None:
            # Advanced mode: user supplies their own Provider.
            self.provider = provider

        elif chain_id is None:
            # Manual mode: endpoints required, no discovery.
            urls = list(endpoints or [])
            if not urls:
                raise NoEndpoints(
                    "No chain_id provided and no endpoints provided. "
                    "Use Web3(<chain_id>) for discovery or Web3(endpoints=[...]) for manual mode."
                )

            self.provider = Provider(
                urls,
                retry_policy_read=self.config.retry_policy_read,
                retry_policy_write=self.config.retry_policy_write,
            )

        else:
            # Discovery (or hybrid) mode.
            # If endpoints are provided, they are injected as priority endpoints.
            self.provider = provider_for_chain(
                int(chain_id),
                priority_endpoints=endpoints,
                retry_policy_read=self.config.retry_policy_read,
                retry_policy_write=self.config.retry_policy_write,
            )

        # Namespaces
        self.eth = Eth(self)

    def close(self) -> None:
        self.provider.close()

    def make_request(
        self, method: str, params: list[Any], *, kind: str = "read", formatter=None
    ) -> Any:
        """
        Perform a raw JSON-RPC request.

        method: e.g. "eth_getBalance"
        params: JSON-RPC params list. No validation is performed on this list.
        kind: "read" or "write" (routing hint)
        formatter: optional post-processor for result
        """

        def bg_func(h: Handle) -> None:
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
      - In strict mode, addresses/hashes/data/topics/quantities are validated; checksum NOT enforced
      - Structured outputs (dict/list) are normalized via normalize_rpc_obj.
    """

    def __init__(self, w3: "Web3") -> None:
        self._w3 = w3

    # ----------------------------
    # builders
    # ----------------------------

    def _tx_object(
        self,
        *,
        from_: str | bytes | None = None,
        to: str | bytes | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | bytes | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        strict = bool(self._w3.config.strict)
        tx: dict[str, Any] = {}

        if from_ is not None:
            tx["from"] = validation.normalize_address(from_, strict=strict)
        if to is not None:
            tx["to"] = validation.normalize_address(to, strict=strict)
        if gas is not None:
            tx["gas"] = validation.quantity(gas, strict=strict)
        if gas_price is not None:
            tx["gasPrice"] = validation.quantity(gas_price, strict=strict)
        if max_fee_per_gas is not None:
            tx["maxFeePerGas"] = validation.quantity(max_fee_per_gas, strict=strict)
        if max_priority_fee_per_gas is not None:
            tx["maxPriorityFeePerGas"] = validation.quantity(
                max_priority_fee_per_gas, strict=strict
            )
        if value is not None:
            tx["value"] = validation.quantity(value, strict=strict)
        if data is not None:
            tx["data"] = validation.data_hex(data, name="data", strict=strict, allow_empty=True)
        if nonce is not None:
            tx["nonce"] = validation.quantity(nonce, strict=strict)
        if chain_id is not None:
            tx["chainId"] = validation.quantity(chain_id, strict=strict)
        if type_ is not None:
            tx["type"] = validation.quantity(type_, strict=strict)
        if access_list is not None:
            tx["accessList"] = access_list

        validation.validate_tx_object(tx, strict=strict)
        return tx

    def _filter_object(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | bytes | list[str | bytes] | None = None,
        topics: list[str | bytes | list[str | bytes] | None] | None = None,
        block_hash: str | bytes | None = None,
    ) -> dict[str, Any]:
        strict = bool(self._w3.config.strict)
        flt: dict[str, Any] = {}

        if block_hash is not None:
            flt["blockHash"] = validation.hash32(block_hash, name="block_hash", strict=strict)

        if from_block is not None:
            flt["fromBlock"] = validation.block_id(from_block, strict=strict)
        if to_block is not None:
            flt["toBlock"] = validation.block_id(to_block, strict=strict)
        if address is not None:
            if isinstance(address, list):
                flt["address"] = [validation.normalize_address(a, strict=strict) for a in address]
            else:
                flt["address"] = validation.normalize_address(address, strict=strict)
        if topics is not None:
            flt["topics"] = validation.topics(topics, strict=strict)

        validation.validate_filter_object(flt, strict=strict)
        return flt

    def protocol_version(self) -> str:
        return self._w3.make_request("eth_protocolVersion", [])

    def syncing(self) -> bool | dict[str, Any]:
        return self._w3.make_request("eth_syncing", [], formatter=normalize_rpc_obj)

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

    def get_balance(self, address: str | bytes, block: BlockId = "latest") -> int:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request("eth_getBalance", [addr, blk], formatter=to_int)

    def get_storage_at(
        self, address: str | bytes, position: int | str, block: BlockId = "latest"
    ) -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        pos = validation.quantity(position, strict=strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request("eth_getStorageAt", [addr, pos, blk])

    def get_transaction_count(self, address: str | bytes, block: BlockId = "latest") -> int:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request(
            "eth_getTransactionCount",
            [addr, blk],
            formatter=to_int,
        )

    def get_block_transaction_count_by_hash(self, block_hash: str | bytes) -> int:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        return self._w3.make_request("eth_getBlockTransactionCountByHash", [h], formatter=to_int)

    def get_block_transaction_count_by_number(self, block: BlockId) -> int:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request(
            "eth_getBlockTransactionCountByNumber",
            [blk],
            formatter=to_int,
        )

    def get_uncle_count_by_block_hash(self, block_hash: str | bytes) -> int:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        return self._w3.make_request("eth_getUncleCountByBlockHash", [h], formatter=to_int)

    def get_uncle_count_by_block_number(self, block: BlockId) -> int:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request(
            "eth_getUncleCountByBlockNumber",
            [blk],
            formatter=to_int,
        )

    def get_code(self, address: str | bytes, block: BlockId = "latest") -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request("eth_getCode", [addr, blk])

    def sign(self, address: str | bytes, data: str | bytes) -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        payload = validation.data_hex(data, name="data", strict=strict, allow_empty=True)
        return self._w3.make_request("eth_sign", [addr, payload], kind="write")

    def sign_transaction(
        self,
        *,
        from_: str | bytes | None = None,
        to: str | bytes | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | bytes | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        strict = bool(self._w3.config.strict)
        if strict:
            if from_ is None:
                raise ValidationError("eth_signTransaction requires 'from'")
            if to is None and data is None:
                raise ValidationError("eth_signTransaction requires at least one of 'to' or 'data'")

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
        from_: str | bytes | None = None,
        to: str | bytes | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | bytes | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
    ) -> str:
        strict = bool(self._w3.config.strict)
        if strict:
            if from_ is None:
                raise ValidationError("eth_sendTransaction requires 'from'")
            if to is None and data is None:
                raise ValidationError("eth_sendTransaction requires at least one of 'to' or 'data'")

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

    def send_raw_transaction(self, signed_tx: str | bytes) -> str:
        strict = bool(self._w3.config.strict)
        tx = validation.data_hex(signed_tx, name="signed_tx", strict=strict, allow_empty=False)
        return self._w3.make_request("eth_sendRawTransaction", [tx], kind="write")

    def call(
        self,
        *,
        to: str | bytes | None = None,
        from_: str | bytes | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | bytes | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
        block: BlockId = "latest",
    ) -> str:
        strict = bool(self._w3.config.strict)
        if strict and to is None and data is None:
            raise ValidationError("eth_call requires at least one of 'to' or 'data'")

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
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request("eth_call", [tx, blk])

    def estimate_gas(
        self,
        *,
        to: str | bytes | None = None,
        from_: str | bytes | None = None,
        gas: int | str | None = None,
        gas_price: int | str | None = None,
        max_fee_per_gas: int | str | None = None,
        max_priority_fee_per_gas: int | str | None = None,
        value: int | str | None = None,
        data: str | bytes | None = None,
        nonce: int | str | None = None,
        chain_id: int | str | None = None,
        type_: int | str | None = None,
        access_list: list[Mapping[str, Any]] | None = None,
        block: BlockId | None = None,
    ) -> int:
        strict = bool(self._w3.config.strict)
        if strict and to is None and data is None:
            raise ValidationError("eth_estimateGas requires at least one of 'to' or 'data'")

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
            params.append(validation.block_id(block, strict=strict))
        return self._w3.make_request("eth_estimateGas", params, formatter=to_int)

    def get_block_by_hash(
        self, block_hash: str | bytes, full_transactions: bool = False
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        return self._w3.make_request(
            "eth_getBlockByHash", [h, full_transactions], formatter=normalize_rpc_obj
        )

    def get_block_by_number(
        self, block: BlockId, full_transactions: bool = False
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        return self._w3.make_request(
            "eth_getBlockByNumber", [blk, full_transactions], formatter=normalize_rpc_obj
        )

    def get_transaction_by_hash(self, tx_hash: str | bytes) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(tx_hash, name="tx_hash", strict=strict)
        return self._w3.make_request("eth_getTransactionByHash", [h], formatter=normalize_rpc_obj)

    def get_transaction_by_block_hash_and_index(
        self, block_hash: str | bytes, index: int | str
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        idx = validation.index(index, strict=strict)
        return self._w3.make_request(
            "eth_getTransactionByBlockHashAndIndex", [h, idx], formatter=normalize_rpc_obj
        )

    def get_transaction_by_block_number_and_index(
        self, block: BlockId, index: int | str
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        idx = validation.index(index, strict=strict)
        return self._w3.make_request(
            "eth_getTransactionByBlockNumberAndIndex", [blk, idx], formatter=normalize_rpc_obj
        )

    def get_transaction_receipt(self, tx_hash: str | bytes) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(tx_hash, name="tx_hash", strict=strict)
        return self._w3.make_request("eth_getTransactionReceipt", [h], formatter=normalize_rpc_obj)

    def get_uncle_by_block_hash_and_index(
        self, block_hash: str | bytes, index: int | str
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        idx = validation.index(index, strict=strict)
        return self._w3.make_request(
            "eth_getUncleByBlockHashAndIndex", [h, idx], formatter=normalize_rpc_obj
        )

    def get_uncle_by_block_number_and_index(
        self, block: BlockId, index: int | str
    ) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        idx = validation.index(index, strict=strict)
        return self._w3.make_request(
            "eth_getUncleByBlockNumberAndIndex", [blk, idx], formatter=normalize_rpc_obj
        )

    def new_filter(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | bytes | list[str | bytes] | None = None,
        topics: list[str | bytes | list[str | bytes] | None] | None = None,
        block_hash: str | bytes | None = None,
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
        # could be list of logs or list of hashes, depending on filter type/client
        return self._w3.make_request(
            "eth_getFilterChanges", [filter_id], formatter=normalize_rpc_obj
        )

    def get_filter_logs(self, filter_id: str) -> list[Any]:
        return self._w3.make_request("eth_getFilterLogs", [filter_id], formatter=normalize_rpc_obj)

    def get_logs(
        self,
        *,
        from_block: BlockId | None = None,
        to_block: BlockId | None = None,
        address: str | bytes | list[str | bytes] | None = None,
        topics: list[str | bytes | list[str | bytes] | None] | None = None,
        block_hash: str | bytes | None = None,
    ) -> list[Any]:
        flt = self._filter_object(
            from_block=from_block,
            to_block=to_block,
            address=address,
            topics=topics,
            block_hash=block_hash,
        )
        return self._w3.make_request("eth_getLogs", [flt], formatter=normalize_rpc_obj)
