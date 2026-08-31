#!/usr/bin/env bash
#
# The webhook flow end to end: a customer, an invoice, a charge that succeeds, a charge that is
# declined, and the signed deliveries all of that produced -- shown from both sides of the wire.
#
#   just demo
#
# Needs curl and jq. The stack must already be up (`just up`); this script only drives it.

set -euo pipefail

BUSINESS=${BUSINESS:-http://localhost:3001}
RECEIVER=${RECEIVER:-http://localhost:3002}
TOKEN=${TOKEN:-dev-token}

# The business's own back office is gated; the payer's two endpoints are not, which is why the
# charges below carry no token.
operator() { curl -sS -H "X-API-Token: $TOKEN" "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# A unique email per run, so the demo can be run twice without tripping the UNIQUE constraint.
EMAIL="ada+$(date +%s)@example.com"

step "Customer"
CUSTOMER=$(operator -X POST "$BUSINESS/customers" \
    -H 'content-type: application/json' \
    -d "{\"name\": \"Ada Lovelace\", \"email\": \"$EMAIL\"}")
echo "$CUSTOMER" | jq .
CUSTOMER_ID=$(echo "$CUSTOMER" | jq -r .id)

# Raises an invoice and finalises it, echoing the created state. Every one of these emits
# invoice.created; readying it does not, since the three specified events are the lifecycle
# points a receiver actually bills on.
raise() {
    local invoice
    invoice=$(operator -X POST "$BUSINESS/invoices" \
        -H 'content-type: application/json' \
        -d "{\"customer_id\": \"$CUSTOMER_ID\", \"due_date\": \"2026-10-01\",
             \"line_items\": [{\"description\": \"$1\", \"quantity\": 1,
                               \"unit_amount_cents\": $2}]}")
    echo "$invoice" | jq . >&2
    local id
    id=$(echo "$invoice" | jq -r .id)
    operator -X POST "$BUSINESS/invoices/$id/ready" >/dev/null
    echo "$id"
}

step "Invoice to be paid  ->  invoice.created"
PAID_INVOICE=$(raise "Consulting" 12500)

step "Invoice to be declined  ->  invoice.created"
DECLINED_INVOICE=$(raise "Retainer" 4999)

# Idempotency-Key is required on this endpoint: one key, one response, forever.
charge() {
    curl -sS -o /dev/stderr -w '%{http_code}\n' \
        -X POST "$BUSINESS/invoices/$1/pay" \
        -H 'content-type: application/json' \
        -H "Idempotency-Key: demo-$(date +%s)-$RANDOM" \
        -d "{\"card_token\": \"$2\"}"
}

step "Successful charge  ->  invoice.paid  (expect 200)"
echo "HTTP $(charge "$PAID_INVOICE" tok_success)"

step "Declined charge  ->  invoice.payment_failed  (expect 402)"
echo "HTTP $(charge "$DECLINED_INVOICE" tok_card_declined)"

# The API answered without ever touching a receiver: delivery is queued in the same transaction
# as the state change and handed to a background task, which sweeps for due deliveries once a
# second. The wait below is for that sweep, not for anything the receiver does.
step "Waiting 5s for the deliveries to go out"
sleep 5

step "Events raised (GET /events -- how a business reconciles what it missed)"
operator "$BUSINESS/events" | jq '[.[] | {id, type, invoice: .data.invoice.id, state: .data.invoice.state}]'

step "Deliveries (GET /webhook_deliveries -- one row per event, per endpoint)"
operator "$BUSINESS/webhook_deliveries" |
    jq '[.[] | {event_type, status, attempts, last_status, last_error}]'

step "Received (GET /received on webhook-receiver -- the other side of the wire)"
curl -sS "$RECEIVER/received" |
    jq '[.[] | {type: .body.type, attempt, verified, duplicate, rejected}]'

step "Summary"
DELIVERED=$(operator "$BUSINESS/webhook_deliveries" | jq '[.[] | select(.status == "succeeded")] | length')
RETRIED=$(operator "$BUSINESS/webhook_deliveries" | jq '[.[] | select(.attempts > 1)] | length')
VERIFIED=$(curl -sS "$RECEIVER/received" | jq '[.[] | select(.verified)] | length')
echo "delivered: $DELIVERED    needed a retry: $RETRIED    signature verified by receiver: $VERIFIED"
