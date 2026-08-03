#!/bin/bash
# ============================================================================
#  test-oauth.sh — проверка схемы OAuth 2.0 для Kafka по шагам.
#  Каждый шаг изолирован: если что-то не работает, сразу видно ЧТО именно.
#  Те же шаги применимы к «Фактор ЕСБ» — достаточно поменять переменные.
# ============================================================================
set -uo pipefail

BASE="${BASE:-/opt/kafka-oauth-stand}"
KAFKA_HOME="${KAFKA_HOME:-$(ls -d "$BASE"/kafka_* 2>/dev/null | head -1)}"
ISSUER="${ISSUER:-https://$(hostname -f):8443}"
TOKEN_URL="${TOKEN_URL:-$ISSUER/token}"
JWKS_URL="${JWKS_URL:-$ISSUER/jwks}"
CLIENT_ID="${CLIENT_ID:-kafka-client}"
CLIENT_SECRET="${CLIENT_SECRET:-client-secret}"
SCOPE="${SCOPE:-kafka:read kafka:write}"
CACERT="${CACERT:-$BASE/tls/ca.crt}"
BOOTSTRAP="${BOOTSTRAP:-$(hostname -f):9092}"
TOPIC="${TOPIC:-oauth-test}"

ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
bad()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*"; FAIL=1; }
step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
FAIL=0
CURL="curl -sS --cacert $CACERT"

# ---------------------------------------------------------------------------
step "1. Discovery-документ провайдера"
DISC=$($CURL "$ISSUER/.well-known/openid-configuration" 2>&1)
if echo "$DISC" | grep -q '"jwks_uri"'; then
  ok "discovery доступен"
  echo "$DISC" | python3 -m json.tool | sed 's/^/    /'
else
  bad "discovery недоступен: $DISC"
  echo "    (не критично — Kafka работает и без discovery, нужны только token и jwks URL)"
fi

# ---------------------------------------------------------------------------
step "2. JWKS: брокер должен уметь скачать публичные ключи"
JWKS=$($CURL "$JWKS_URL" 2>&1)
if echo "$JWKS" | grep -q '"keys"'; then
  ok "JWKS доступен, ключей: $(echo "$JWKS" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["keys"]))')"
  echo "$JWKS" | python3 -c 'import sys,json
for k in json.load(sys.stdin)["keys"]:
    print("    kid=%s kty=%s alg=%s use=%s" % (k.get("kid"),k.get("kty"),k.get("alg"),k.get("use")))'
else
  bad "JWKS недоступен: $JWKS"
fi

# ---------------------------------------------------------------------------
step "3. Получение токена по grant_type=client_credentials"
RESP=$($CURL -u "$CLIENT_ID:$CLIENT_SECRET" \
       -d "grant_type=client_credentials" --data-urlencode "scope=$SCOPE" \
       "$TOKEN_URL" 2>&1)
TOKEN=$(echo "$RESP" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("access_token",""))
except Exception: print("")' 2>/dev/null)
if [ -n "$TOKEN" ]; then
  ok "токен получен (${#TOKEN} символов)"
else
  bad "токен не получен: $RESP"
fi

# ---------------------------------------------------------------------------
step "4. Разбор claim'ов токена (то, что будет проверять брокер)"
if [ -n "$TOKEN" ]; then
  python3 - "$TOKEN" <<'PY' | sed 's/^/    /'
import sys, json, base64, time
h, p, _ = sys.argv[1].split(".")
d = lambda x: base64.urlsafe_b64decode(x + "=" * (-len(x) % 4))
hdr = json.loads(d(h)); cl = json.loads(d(p))
print("header :", json.dumps(hdr))
print("claims :", json.dumps(cl, indent=2, ensure_ascii=False))
must = {"iss": "sasl.oauthbearer.expected.issuer",
        "aud": "sasl.oauthbearer.expected.audience",
        "sub": "sasl.oauthbearer.sub.claim.name -> принципал ACL",
        "exp": "срок жизни"}
print("\nсоответствие требованиям Kafka:")
for c, why in must.items():
    print(("  OK   " if c in cl else "  НЕТ  ") + f"{c:5s} — {why}")
if hdr.get("alg") not in ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256"):
    print(f"  ВНИМАНИЕ: alg={hdr.get('alg')} — Kafka требует асимметричную подпись")
if "exp" in cl:
    print(f"  TTL   : {cl['exp'] - cl.get('iat', int(time.time()))} сек")
PY
else
  bad "нет токена — шаг пропущен"
fi

# ---------------------------------------------------------------------------
step "5. Проверка подписи токена ключом из JWKS"
if [ -n "$TOKEN" ]; then
  python3 - "$TOKEN" "$JWKS_URL" "$CACERT" <<'PY' | sed 's/^/    /'
import sys, json, base64, ssl, urllib.request
from cryptography.hazmat.primitives.asymmetric import rsa, padding, ec, utils
from cryptography.hazmat.primitives import hashes
tok, jwks_url, ca = sys.argv[1], sys.argv[2], sys.argv[3]
h, p, s = tok.split(".")
d = lambda x: base64.urlsafe_b64decode(x + "=" * (-len(x) % 4))
ctx = ssl.create_default_context(cafile=ca) if jwks_url.startswith("https") else None
keys = json.load(urllib.request.urlopen(jwks_url, context=ctx))["keys"]
kid = json.loads(d(h)).get("kid")
jwk = next((k for k in keys if k.get("kid") == kid), keys[0])
signing_input = f"{h}.{p}".encode()
if jwk["kty"] == "RSA":
    n = int.from_bytes(d(jwk["n"]), "big"); e = int.from_bytes(d(jwk["e"]), "big")
    rsa.RSAPublicNumbers(e, n).public_key().verify(
        d(s), signing_input, padding.PKCS1v15(), hashes.SHA256())
else:
    raise SystemExit(f"kty={jwk['kty']} — проверьте вручную")
print("подпись валидна, kid =", jwk.get("kid"))
PY
  [ $? -eq 0 ] && ok "подпись проверяется ключом из JWKS" || bad "подпись не проверилась"
fi

# ---------------------------------------------------------------------------
step "6. Аутентификация в Kafka (создание топика от имени OAuth-принципала)"
if [ -z "$KAFKA_HOME" ] || [ ! -x "$KAFKA_HOME/bin/kafka-topics.sh" ]; then
  echo "  KAFKA_HOME не найден — шаги 6-8 пропущены"
else
  export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=$TOKEN_URL,$JWKS_URL"
  CFG="$BASE/config/client-oauth.properties"
  OUT=$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
        --command-config "$CFG" --list 2>&1)
  if echo "$OUT" | grep -qiE 'SaslAuthenticationException|Authentication failed'; then
    bad "аутентификация не прошла"; echo "$OUT" | tail -5 | sed 's/^/    /'
  elif echo "$OUT" | grep -qi 'TopicAuthorizationException\|not authorized'; then
    ok "аутентификация прошла (отказ на авторизации — нужны ACL)"
  else
    ok "аутентификация и авторизация прошли"
  fi

  step "7. Выдача ACL на принципал из claim sub"
  "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" \
    --command-config "$BASE/config/client-oauth.properties" \
    --add --allow-principal "User:$CLIENT_ID" \
    --operation Read --operation Write --operation Describe --operation Create \
    --topic "$TOPIC" 2>&1 | tail -3 | sed 's/^/    /'

  step "8. Produce/consume"
  echo "hello-oauth" | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" --producer.config "$CFG" --topic "$TOPIC" 2>&1 | tail -3
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
    --consumer.config "$CFG" --topic "$TOPIC" --from-beginning --max-messages 1 \
    --timeout-ms 15000 2>&1 | tail -5 | sed 's/^/    /'
fi

printf '\n'
[ "$FAIL" = 0 ] && printf '\033[1;32mВсе проверенные шаги прошли\033[0m\n' \
                || printf '\033[1;31mЕсть ошибки — см. выше\033[0m\n'
exit "$FAIL"
