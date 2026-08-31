#!/usr/bin/env bash
#
# The five payment failure modes, driven end to end against the running stack. Each section is
# one of the cases in DESIGN.md section 3, in the same order:
#
#   (a) two clients charging one invoice at the same instant
#   (b) a payment service that never answers (tok_timeout, 30s against a 5s client timeout)
#   (c) a charge that lands and whose response is lost (tok_network_error)
#   (d) an idempotency key reused with a different body
#   (e) a second charge against an invoice that is already paid
#
#   just verify-payments
#
# Needs curl and jq. The stack must already be up (`just up`); this script only drives it.
#
# Case (c) leaves an invoice held in `processing` on purpose -- that is the state the reconciler
# exists to resolve, and the last section says how to watch it do so.

set -euo pipefail

BUSINESS=${BUSINESS:-http://localhost:3001}
PSP=${PSP:-http://localhost:3000}
TOKEN=${TOKEN:-dev-token}

operator() { curl -sS -H "X-API-Token: $TOKEN" "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# A unique suffix per run, so the script can be run twice without colliding on an email or a key.
RUN=$(date +%s)$$

# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

# A fresh invoice in `ready`, which is the only state a charge is accepted from.
new_invoice() {
    local id
    id=$(operator -X POST "$BUSINESS/invoices" -H 'Content-Type: application/json' -d "{
        \"customer_id\": \"$CUSTOMER\",
        \"due_date\": \"2026-12-31\",
        \"line_items\": [{\"description\": \"Consulting\", \"quantity\": 1, \"unit_amount_cents\": 5000}]
    }" | jq -r .id)
    operator -o /dev/null -X POST "$BUSINESS/invoices/$id/ready"
    echo "$id"
}

# A charge. No token: the payer's endpoints are open by necessity, since whoever is being billed
# has none. Prints "<status> <body>" so a caller can read both.
pay() {
    local invoice=$1 key=$2 card=$3 body status
    body=$(mktemp)
    status=$(curl -sS -o "$body" -w '%{http_code}' -X POST "$BUSINESS/invoices/$invoice/pay" \
        -H 'Content-Type: application/json' \
        -H "Idempotency-Key: $key" \
        -d "{\"card_token\": \"$card\"}")
    printf '%s %s' "$status" "$(cat "$body")"
    rm -f "$body"
}

invoice_state() { operator "$BUSINESS/invoices/$1" | jq -c '{state}'; }
intent_status() { curl -sS "$BUSINESS/payment_intents/$1" | jq -c '{status}'; }
psp_truth()     { curl -sS "$PSP/payment_intents/$1" | jq -c '{status}'; }
intent_of()     { sed 's/^[0-9]* //' <<<"$1" | jq -r .payment_intent_id; }

step "Customer"
CUSTOMER=$(operator -X POST "$BUSINESS/customers" -H 'Content-Type: application/json' \
    -d "{\"name\": \"Verification $RUN\", \"email\": \"verify+$RUN@example.com\"}" | jq -r .id)
echo "  $CUSTOMER"

# ---------------------------------------------------------------------------------------
step "(a) Two clients charging one invoice at the same instant"
# ---------------------------------------------------------------------------------------
#
# Distinct keys, so neither is a retry of the other and the idempotency table cannot be what
# saves this. Exactly one may claim the invoice; the other must be refused without charging.

A=$(new_invoice)
echo "  invoice $A"
pay "$A" "a1-$RUN" tok_success >/tmp/verify-a1.$$ &
pay "$A" "a2-$RUN" tok_success >/tmp/verify-a2.$$ &
wait
echo "  client 1: $(cat /tmp/verify-a1.$$)"
echo "  client 2: $(cat /tmp/verify-a2.$$)"
echo "  invoice:  $(invoice_state "$A")"
rm -f /tmp/verify-a1.$$ /tmp/verify-a2.$$

# ---------------------------------------------------------------------------------------
step "(b) The payment service never answers (tok_timeout)"
# ---------------------------------------------------------------------------------------
#
# 30 seconds at the PSP against a 5 second client timeout. The charge may or may not have gone
# through, so nothing is written and the invoice is held rather than released.

B=$(new_invoice)
echo "  invoice $B"
START=$(date +%s)
B_RESULT=$(pay "$B" "b1-$RUN" tok_timeout)
echo "  response after $(( $(date +%s) - START ))s: $B_RESULT"
B_INTENT=$(intent_of "$B_RESULT")
echo "  invoice:         $(invoice_state "$B")"
echo "  payment_intent:  $(intent_status "$B_INTENT")   <- 'we do not know yet', not 'not charged'"
echo "  retry, same key: $(pay "$B" "b1-$RUN" tok_timeout)"

# ---------------------------------------------------------------------------------------
step "(c) The charge lands and the response is lost (tok_network_error)"
# ---------------------------------------------------------------------------------------
#
# The PSP records the charge as succeeded and then drops the connection. Indistinguishable from
# a timeout at this end, which is the point: both are 'the money may have moved'.

C=$(new_invoice)
echo "  invoice $C"
C_RESULT=$(pay "$C" "c1-$RUN" tok_network_error)
echo "  response:        $C_RESULT"
C_INTENT=$(intent_of "$C_RESULT")
echo "  invoice:         $(invoice_state "$C")"
echo "  our record:      $(intent_status "$C_INTENT")"
echo "  PSP's truth:     $(psp_truth "$C_INTENT")   <- the money DID move"
echo "  retry, same key: $(pay "$C" "c1-$RUN" tok_network_error)   <- replayed, PSP untouched"
echo "  retry, new key:  $(pay "$C" "c2-$RUN" tok_success)   <- refused, invoice is held"
echo "  our record:      $(intent_status "$C_INTENT")   <- neither retry changed anything"

# ---------------------------------------------------------------------------------------
step "(d) An idempotency key reused with a different body"
# ---------------------------------------------------------------------------------------
#
# The first charge is declined, which returns the invoice to `ready`. So the refusal below is on
# the key alone -- nothing about the invoice would have stopped a second charge.

D=$(new_invoice)
echo "  invoice $D"
echo "  first, declined card: $(pay "$D" "d1-$RUN" tok_card_declined)"
echo "  same key, new body:   $(pay "$D" "d1-$RUN" tok_success)"
echo "  invoice:              $(invoice_state "$D")   <- payable, and still refused"

# ---------------------------------------------------------------------------------------
step "(e) A second charge against an invoice that is already paid"
# ---------------------------------------------------------------------------------------
#
# A new key is a new intended charge and is refused. The original key is a caller asking 'did my
# request land?', and is answered with the response it already got.

E=$(new_invoice)
echo "  invoice $E"
echo "  first:           $(pay "$E" "e1-$RUN" tok_success)"
echo "  invoice:         $(invoice_state "$E")"
echo "  again, new key:  $(pay "$E" "e2-$RUN" tok_success)   <- duplicate"
echo "  again, same key: $(pay "$E" "e1-$RUN" tok_success)   <- retry, replayed"

# ---------------------------------------------------------------------------------------
step "Left for the reconciler"
# ---------------------------------------------------------------------------------------
#
# Cases (b) and (c) are still held. The reconciler's first tick is at startup, so restarting the
# service is how to watch it resolve them rather than waiting out the daily interval.

cat <<EOF
  invoice $B  $(invoice_state "$B")  intent $B_INTENT  $(intent_status "$B_INTENT")
  invoice $C  $(invoice_state "$C")  intent $C_INTENT  $(intent_status "$C_INTENT")

  Resolve them:
    docker compose restart business
    docker compose logs business | grep reconciler

  (b) charges for 30s at the PSP, so give it that long before restarting or the reconciler
  will correctly find it still 'pending' and leave it for the next pass.
EOF
