"""fastweb3 - High-performance Web3 client."""

from .errors import FastWeb3Error, RPCError, TransportError
from .web3 import Web3, Web3Config

__all__ = [
    "Web3",
    "Web3Config",
    "FastWeb3Error",
    "RPCError",
    "TransportError",
]
