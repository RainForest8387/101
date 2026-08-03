#!/usr/bin/env python3
"""
mock-idp.py — минимальный OAuth 2.0 / OIDC провайдер для проверки схемы
SASL/OAUTHBEARER в Apache Kafka ДО подключения к «Фактор ЕСБ».

Реализует ровно тот минимум, который требует Kafka:
  GET  /.well-known/openid-configuration  — discovery
  GET  /jwks                              — JSON Web Key Set (RS256)
  POST /token                             — grant_type=client_credentials
  POST /token                             — grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer

Зависимость: python3 -m pip install cryptography
             (в ALT Linux: apt-get install python3-module-cryptography)

Запуск:
  ./mock-idp.py --port 8080
  ./mock-idp.py --port 8443 --tls-cert idp.crt --tls-key idp.key

Клиенты задаются через --client ID:SECRET:SCOPE (можно несколько раз).
По умолчанию: kafka-broker / broker-secret и kafka-client / client-secret.
"""

import argparse
import base64
import json
import hashlib
import ssl
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
except ImportError:
    sys.exit("Нужен пакет cryptography: pip install cryptography "
             "(ALT: apt-get install python3-module-cryptography)")


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64u_int(value: int) -> str:
    length = (value.bit_length() + 7) // 8
    return b64u(value.to_bytes(length, "big"))


class Signer:
    """RSA-ключ + выпуск и подпись JWT (RS256)."""

    def __init__(self, key_path=None):
        if key_path:
            with open(key_path, "rb") as fh:
                self.key = serialization.load_pem_private_key(fh.read(), password=None)
        else:
            self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pub = self.key.public_key().public_numbers()
        self.n, self.e = pub.n, pub.e
        # kid — стабильный отпечаток открытого ключа
        self.kid = hashlib.sha256(
            self.key.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo)
        ).hexdigest()[:16]

    def jwks(self) -> dict:
        return {"keys": [{
            "kty": "RSA", "use": "sig", "alg": "RS256", "kid": self.kid,
            "n": b64u_int(self.n), "e": b64u_int(self.e),
        }]}

    def sign(self, claims: dict) -> str:
        header = {"alg": "RS256", "typ": "JWT", "kid": self.kid}
        signing_input = (
            b64u(json.dumps(header, separators=(",", ":")).encode())
            + "." + b64u(json.dumps(claims, separators=(",", ":")).encode())
        ).encode("ascii")
        sig = self.key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        return signing_input.decode("ascii") + "." + b64u(sig)


class Handler(BaseHTTPRequestHandler):
    server_version = "mock-idp/1.0"

    # --- служебное ---------------------------------------------------------
    def _send(self, code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        sys.stderr.write("[idp] %s - %s\n" % (self.address_string(), fmt % args))

    # --- GET ---------------------------------------------------------------
    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        cfg = self.server.cfg
        if path == "/.well-known/openid-configuration":
            self._send(200, {
                "issuer": cfg["issuer"],
                "token_endpoint": cfg["issuer"] + "/token",
                "jwks_uri": cfg["issuer"] + "/jwks",
                "grant_types_supported": [
                    "client_credentials",
                    "urn:ietf:params:oauth:grant-type:jwt-bearer",
                ],
                "id_token_signing_alg_values_supported": ["RS256"],
                "token_endpoint_auth_methods_supported": [
                    "client_secret_basic", "client_secret_post",
                ],
            })
        elif path == "/jwks":
            self._send(200, self.server.signer.jwks())
        else:
            self._send(404, {"error": "not_found"})

    # --- POST --------------------------------------------------------------
    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path != "/token":
            return self._send(404, {"error": "not_found"})

        length = int(self.headers.get("Content-Length", 0))
        form = urllib.parse.parse_qs(self.rfile.read(length).decode())
        grant = form.get("grant_type", [""])[0]
        cfg, signer = self.server.cfg, self.server.signer

        # client_id/secret: либо Basic, либо в теле
        client_id = form.get("client_id", [None])[0]
        client_secret = form.get("client_secret", [None])[0]
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                raw = base64.b64decode(auth[6:]).decode()
                client_id, client_secret = (
                    urllib.parse.unquote(p) for p in raw.split(":", 1))
            except Exception:
                return self._send(400, {"error": "invalid_request"})

        if grant == "client_credentials":
            expected = cfg["clients"].get(client_id)
            if expected is None or expected["secret"] != client_secret:
                return self._send(401, {"error": "invalid_client"})
            scope = form.get("scope", [expected["scope"]])[0]
        elif grant == "urn:ietf:params:oauth:grant-type:jwt-bearer":
            # для стенда подпись assertion не проверяем — только читаем sub
            assertion = form.get("assertion", [""])[0]
            try:
                payload = assertion.split(".")[1]
                payload += "=" * (-len(payload) % 4)
                client_id = json.loads(base64.urlsafe_b64decode(payload))["sub"]
            except Exception:
                return self._send(400, {"error": "invalid_grant"})
            entry = cfg["clients"].get(client_id)
            if entry is None:
                return self._send(401, {"error": "invalid_client"})
            scope = form.get("scope", [entry["scope"]])[0]
        else:
            return self._send(400, {"error": "unsupported_grant_type"})

        now = int(time.time())
        claims = {
            "iss": cfg["issuer"],
            "sub": client_id,
            "aud": cfg["audience"],
            "iat": now,
            "nbf": now,
            "exp": now + cfg["ttl"],
            "scope": scope,
            "client_id": client_id,
        }
        token = signer.sign(claims)
        self._send(200, {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": cfg["ttl"],
            "scope": scope,
        })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--issuer", default=None,
                    help="значение claim iss и базовый URL (по умолчанию http://<host>:<port>)")
    ap.add_argument("--audience", default="kafka")
    ap.add_argument("--ttl", type=int, default=300, help="время жизни токена, сек")
    ap.add_argument("--key", default=None, help="PEM с RSA-ключом (иначе генерируется)")
    ap.add_argument("--tls-cert", default=None)
    ap.add_argument("--tls-key", default=None)
    ap.add_argument("--client", action="append", default=[],
                    metavar="ID:SECRET:SCOPE")
    args = ap.parse_args()

    clients = {}
    for spec in args.client:
        parts = spec.split(":")
        if len(parts) != 3:
            sys.exit("--client требует формат ID:SECRET:SCOPE")
        clients[parts[0]] = {"secret": parts[1], "scope": parts[2]}
    if not clients:
        clients = {
            "kafka-broker": {"secret": "broker-secret", "scope": "kafka:broker"},
            "kafka-client": {"secret": "client-secret", "scope": "kafka:read kafka:write"},
            "kafka-ui":     {"secret": "ui-secret",     "scope": "kafka:admin"},
        }

    scheme = "https" if args.tls_cert else "http"
    issuer = args.issuer or f"{scheme}://localhost:{args.port}"

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    httpd.signer = Signer(args.key)
    httpd.cfg = {"issuer": issuer, "audience": args.audience,
                 "ttl": args.ttl, "clients": clients}

    if args.tls_cert:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.tls_cert, args.tls_key)
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)

    print(f"[idp] issuer   : {issuer}")
    print(f"[idp] jwks_uri : {issuer}/jwks")
    print(f"[idp] token    : {issuer}/token")
    print(f"[idp] audience : {args.audience}")
    print(f"[idp] kid      : {httpd.signer.kid}")
    print(f"[idp] clients  : {', '.join(clients)}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
