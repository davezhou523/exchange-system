API_KEY='EyYZXotRJaaXeycqfwLaDYBREyLTyNQK8lCuZKilieOg5gBgKGnpbdXBSMN7eMjJ'
SECRET_KEY='69wfjz9tkqUykbwlla6HUYcCKuVtRKvOEYoCUC1BdA0dYChSOccJpI1xnUpId0UP'
SYMBOL='ETHUSDT'
START_TIME=$(python3 - <<'PY'
import time
print(int((time.time() - 7*24*3600) * 1000))
PY
)
TIMESTAMP=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)

QUERY="symbol=${SYMBOL}&limit=500&recvWindow=5000&startTime=${START_TIME}&timestamp=${TIMESTAMP}"
SIGNATURE=$(printf '%s' "$QUERY" | openssl dgst -sha256 -hmac "$SECRET_KEY" | awk '{print $2}')

curl -x socks5://192.168.10.14:1080 \
  -H "X-MBX-APIKEY: ${API_KEY}" \
  "https://demo-fapi.binance.com/fapi/v1/allOrders?${QUERY}&signature=${SIGNATURE}"