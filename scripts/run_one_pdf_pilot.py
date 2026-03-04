#!/usr/bin/env python3
"""Compatibility shim for one-PDF pilot CLI."""

from pension_data.ops.one_pdf_pilot_cli import main

if __name__ == "__main__":
    raise SystemExit(main())
