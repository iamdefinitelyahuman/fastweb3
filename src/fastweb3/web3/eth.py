# src/fastweb3/web3/eth.py
from __future__ import annotations

from typing import Any, Mapping, Union

from .. import validation
from ..errors import ValidationError
from ..formatters import normalize_rpc_obj, to_int

BlockId = Union[
    str, int
]  # "latest" | "pending" | "earliest" | "safe" | "finalized" | hex str | int


def _is_latest_like_block(x: BlockId | None) -> bool:
    # We enforce freshness only for semantics that are implicitly "latest/pending".
    # (If x is None, many RPC methods default to "latest".)
    if x is None:
        return True
    return isinstance(x, str) and x in ("latest", "pending")


def _fresh_latest(_resp: Any, required_tip: int, returned_tip: int) -> bool:
    # Strict monotonicity: never accept a response from a node behind what we knew
    # at the start of the attempt (required_tip is concurrency-safe snapshot).
    return returned_tip >= required_tip


def _fresh_negative_requires_latest(resp: Any, required_tip: int, returned_tip: int) -> bool:
    # For maybe-null lookups: any positive result is acceptable even if the node is behind,
    # but a negative result is only trustworthy when the node is not behind required_tip.
    return (resp is not None) or (returned_tip >= required_tip)


class Eth:
    """
    Core eth_* JSON-RPC namespace as listed on ethereum.org.

    Notes:
      - For "transaction object" params, methods take keyword-only args and build the dict.
      - For "filter object" params, methods take keyword-only args and build the dict.
      - Quantity inputs may be passed as int (we hex-encode) or as already-encoded 0x strings.
      - In strict mode, addresses/hashes/data/topics/quantities are validated; checksum NOT enforced
      - Structured outputs (dict/list) are normalized via normalize_rpc_obj.
      - Node-local / stateful methods (filters, local signer, node status) route to primary.
    """

    def __init__(self, w3) -> None:
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

    # ----------------------------
    # basic / node-local
    # ----------------------------

    def protocol_version(self) -> str:
        return self._w3.make_request("eth_protocolVersion", [])

    def syncing(self) -> bool | dict[str, Any]:
        return self._w3.make_request(
            "eth_syncing", [], route="primary", formatter=normalize_rpc_obj
        )

    def coinbase(self) -> str:
        return self._w3.make_request("eth_coinbase", [], route="primary")

    def chain_id(self) -> int:
        return self._w3.make_request("eth_chainId", [], formatter=to_int)

    def mining(self) -> bool:
        return self._w3.make_request("eth_mining", [], route="primary")

    def hashrate(self) -> int:
        return self._w3.make_request("eth_hashrate", [], route="primary", formatter=to_int)

    def gas_price(self) -> int:
        return self._w3.make_request("eth_gasPrice", [], formatter=to_int)

    def accounts(self) -> list[str]:
        return self._w3.make_request("eth_accounts", [], route="primary")

    def block_number(self) -> int:
        return self._w3.make_request("eth_blockNumber", [], formatter=to_int)

    # ----------------------------
    # chain-state
    # ----------------------------

    def get_balance(self, address: str | bytes, block: BlockId = "latest") -> int:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getBalance", [addr, blk], formatter=to_int, freshness=freshness
        )

    def get_storage_at(
        self, address: str | bytes, position: int | str, block: BlockId = "latest"
    ) -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        pos = validation.quantity(position, strict=strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request("eth_getStorageAt", [addr, pos, blk], freshness=freshness)

    def get_transaction_count(self, address: str | bytes, block: BlockId = "latest") -> int:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getTransactionCount",
            [addr, blk],
            formatter=to_int,
            freshness=freshness,
        )

    def get_block_transaction_count_by_hash(self, block_hash: str | bytes) -> int:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        return self._w3.make_request("eth_getBlockTransactionCountByHash", [h], formatter=to_int)

    def get_block_transaction_count_by_number(self, block: BlockId) -> int:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getBlockTransactionCountByNumber",
            [blk],
            formatter=to_int,
            freshness=freshness,
        )

    def get_uncle_count_by_block_hash(self, block_hash: str | bytes) -> int:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(block_hash, name="block_hash", strict=strict)
        return self._w3.make_request("eth_getUncleCountByBlockHash", [h], formatter=to_int)

    def get_uncle_count_by_block_number(self, block: BlockId) -> int:
        strict = bool(self._w3.config.strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getUncleCountByBlockNumber",
            [blk],
            formatter=to_int,
            freshness=freshness,
        )

    def get_code(self, address: str | bytes, block: BlockId = "latest") -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        blk = validation.block_id(block, strict=strict)
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request("eth_getCode", [addr, blk], freshness=freshness)

    # ----------------------------
    # signer / tx submission
    # ----------------------------

    def sign(self, address: str | bytes, data: str | bytes) -> str:
        strict = bool(self._w3.config.strict)
        addr = validation.normalize_address(address, strict=strict)
        payload = validation.data_hex(data, name="data", strict=strict, allow_empty=True)
        return self._w3.make_request("eth_sign", [addr, payload], route="primary")

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
        return self._w3.make_request("eth_signTransaction", [tx], route="primary")

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
        return self._w3.make_request("eth_sendTransaction", [tx], route="primary")

    def send_raw_transaction(self, signed_tx: str | bytes) -> str:
        strict = bool(self._w3.config.strict)
        tx = validation.data_hex(signed_tx, name="signed_tx", strict=strict, allow_empty=False)
        route = "primary" if self._w3.provider.has_primary() else "pool"
        return self._w3.make_request("eth_sendRawTransaction", [tx], route=route)

    # ----------------------------
    # call / estimate
    # ----------------------------

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
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request("eth_call", [tx, blk], freshness=freshness)

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
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_estimateGas", params, formatter=to_int, freshness=freshness
        )

    # ----------------------------
    # structured getters
    # ----------------------------

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
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getBlockByNumber",
            [blk, full_transactions],
            formatter=normalize_rpc_obj,
            freshness=freshness,
        )

    def get_transaction_by_hash(self, tx_hash: str | bytes) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(tx_hash, name="tx_hash", strict=strict)
        return self._w3.make_request(
            "eth_getTransactionByHash",
            [h],
            formatter=normalize_rpc_obj,
            freshness=_fresh_negative_requires_latest,
        )

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
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getTransactionByBlockNumberAndIndex",
            [blk, idx],
            formatter=normalize_rpc_obj,
            freshness=freshness,
        )

    def get_transaction_receipt(self, tx_hash: str | bytes) -> dict[str, Any] | None:
        strict = bool(self._w3.config.strict)
        h = validation.hash32(tx_hash, name="tx_hash", strict=strict)
        return self._w3.make_request(
            "eth_getTransactionReceipt",
            [h],
            formatter=normalize_rpc_obj,
            freshness=_fresh_negative_requires_latest,
        )

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
        freshness = _fresh_latest if _is_latest_like_block(block) else None
        return self._w3.make_request(
            "eth_getUncleByBlockNumberAndIndex",
            [blk, idx],
            formatter=normalize_rpc_obj,
            freshness=freshness,
        )

    # ----------------------------
    # filters (primary-only)
    # ----------------------------

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
        return self._w3.make_request("eth_newFilter", [flt], route="primary")

    def new_block_filter(self) -> str:
        return self._w3.make_request("eth_newBlockFilter", [], route="primary")

    def new_pending_transaction_filter(self) -> str:
        return self._w3.make_request("eth_newPendingTransactionFilter", [], route="primary")

    def uninstall_filter(self, filter_id: str) -> bool:
        return self._w3.make_request("eth_uninstallFilter", [filter_id], route="primary")

    def get_filter_changes(self, filter_id: str) -> list[Any]:
        return self._w3.make_request(
            "eth_getFilterChanges",
            [filter_id],
            route="primary",
            formatter=normalize_rpc_obj,
        )

    def get_filter_logs(self, filter_id: str) -> list[Any]:
        return self._w3.make_request(
            "eth_getFilterLogs",
            [filter_id],
            route="primary",
            formatter=normalize_rpc_obj,
        )

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
        # If to_block is omitted or "latest"/"pending", logs are latest-relative.
        freshness = _fresh_latest if _is_latest_like_block(to_block) else None
        return self._w3.make_request(
            "eth_getLogs",
            [flt],
            formatter=normalize_rpc_obj,
            freshness=freshness,
        )
