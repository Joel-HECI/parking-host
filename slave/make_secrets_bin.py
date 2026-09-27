#!/usr/bin/env python3
"""Create an encrypted LittleFS secrets.bin for the ESP32-CAM sketch.

Output format:
  - 3 bytes:  SS1
  - 1 byte:   version (0x01)
  - 12 bytes: AES-GCM IV
  - 4 bytes:   ciphertext length, little-endian
  - n bytes:   AES-GCM ciphertext
  - 16 bytes:  AES-GCM auth tag

The encryption key is derived from:
  SHA-256(SECRETS_KEY_MATERIAL)

Requirements:
  pip install cryptography

Usage:
  python3 make_secrets_bin.py --ssid ... --wifi-password ... --server-host ... \
    --server-port 8766 --server-path /parking --device-token ... --root-ca root_ca.pem

New option:
  --secrets-key-material STRING
      Optional string used as the key material for deriving the AES key via
      SHA-256. If omitted, the built-in default value is used. Example:

  python3 make_secrets_bin.py --ssid MySSID --wifi-password S3cr3t \
    --server-host example.com --server-port 8766 --server-path /parking \
    --device-token mytoken --root-ca root_ca.pem \
    --secrets-key-material "my long random secret here"
"""

from __future__ import annotations
import os
import argparse
import hashlib
import json
import struct
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SECRETS_KEY_MATERIAL = b"CHANGE_THIS_TO_A_LONG_RANDOM_DEVICE_SECRET"


def derive_key(key_material: bytes | None = None) -> bytes:
  """Derive a 32-byte key from provided key material (or default)."""
  if key_material is None:
    key_material = SECRETS_KEY_MATERIAL
  return hashlib.sha256(key_material).digest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssid", required=True)
    parser.add_argument("--wifi-password", required=True)
    parser.add_argument("--server-host", required=True)
    parser.add_argument("--server-port", required=True, type=int)
    parser.add_argument("--server-path", required=True)
    parser.add_argument("--device-token", required=True)
    parser.add_argument(
      "--secrets-key-material",
      required=False,
      help=("Optional secrets key material string to derive the encryption key from. "
          "If omitted the built-in default is used."),
    )
    parser.add_argument("--root-ca", required=True, help="Path to PEM certificate file")
    parser.add_argument("--out", default="secrets.bin")
    args = parser.parse_args()

    root_ca = Path(args.root_ca).read_text(encoding="utf-8")
    payload = {
        "wifi_ssid": args.ssid,
        "wifi_password": args.wifi_password,
        "server_host": args.server_host,
        "server_port": args.server_port,
        "server_path": args.server_path,
        "device_token": args.device_token,
        "root_ca": root_ca,
    }

    plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    # Use provided secrets key material if given, otherwise fall back to built-in value
    key_material = args.secrets_key_material.encode("utf-8") if args.secrets_key_material else None
    key = derive_key(key_material)
    iv = os.urandom(12)
    aesgcm = AESGCM(key)
    ciphertext_with_tag = aesgcm.encrypt(iv, plaintext, None)
    ciphertext = ciphertext_with_tag[:-16]
    tag = ciphertext_with_tag[-16:]

    out = bytearray()
    out += b"SS1"
    out += bytes([0x01])
    out += iv
    out += struct.pack("<I", len(ciphertext))
    out += ciphertext
    out += tag

    Path(args.out).write_bytes(out)
    print(f"Wrote {args.out} ({len(out)} bytes)")


if __name__ == "__main__":
    main()
