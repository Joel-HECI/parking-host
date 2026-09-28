#!/usr/bin/env python3
"""Compatibility wrapper for the refactored parking host entrypoint."""

from __future__ import annotations

import asyncio

from main import main


if __name__ == "__main__":
    asyncio.run(main())
