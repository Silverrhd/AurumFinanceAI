"""
Legacy OpenFIGI client import for backward compatibility.
Re-exports OpenFIGIClient from the main preprocessing utils package.
"""

from portfolio.preprocessing.utils.openfigi_client import OpenFIGIClient

__all__ = ['OpenFIGIClient']