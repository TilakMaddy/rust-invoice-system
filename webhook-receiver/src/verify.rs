//! The verification recipe, and the only part of this service worth copying.
//!
//! A receiver's whole job on an inbound webhook is to answer one question before it acts on
//! anything: did the service I trust actually send this? Everything here is in service of that,
//! and the order matters as much as the steps.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// How far the signed timestamp may be from now, in seconds, in either direction.
///
/// This is what makes a captured delivery expire. Without it a signature is valid forever, and a
/// recording of one request is a working forgery for as long as the secret lives.
///
/// Either direction, not just the past: a sender whose clock runs fast would otherwise have
/// every delivery accepted indefinitely by a receiver that only checked one side.
const TOLERANCE: i64 = 300;

/// Why a delivery could not be trusted. `None` from [`check`] means it could.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejected {
    /// Nothing is configured for this path, so there is no key to check against. Not the
    /// sender's fault and not a forgery — but not verified either, and it must not be treated as
    /// though it were.
    NoSecretConfigured,
    /// The `webhook-signature` header is absent or not `v1=<hex>`.
    MalformedSignature,
    /// Absent, unparseable, or further from now than [`TOLERANCE`].
    TimestampOutsideTolerance,
    /// Well formed, in the window, and signed with something other than the secret.
    SignatureMismatch,
}

impl Rejected {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSecretConfigured => "no_secret_configured",
            Self::MalformedSignature => "malformed_signature",
            Self::TimestampOutsideTolerance => "timestamp_outside_tolerance",
            Self::SignatureMismatch => "signature_mismatch",
        }
    }
}

/// Whether this delivery really came from the holder of `secret`.
///
/// `body` must be the bytes as they arrived. Parsing the JSON and re-serialising it first is the
/// usual way to end up with a signature that cannot be made to match: key order, whitespace and
/// number formatting are all free to change, and every one of them changes the hash.
pub fn check(
    secret: Option<&str>,
    id: &str,
    timestamp: Option<&str>,
    signature: Option<&str>,
    body: &str,
) -> Option<Rejected> {
    let Some(secret) = secret else {
        return Some(Rejected::NoSecretConfigured);
    };

    // The timestamp first, because it is the cheap check and because a delivery outside the
    // window is refused whatever it is signed with.
    let Some(timestamp) = timestamp else {
        return Some(Rejected::TimestampOutsideTolerance);
    };
    let Ok(sent) = timestamp.parse::<i64>() else {
        return Some(Rejected::TimestampOutsideTolerance);
    };
    if (now() - sent).abs() > TOLERANCE {
        return Some(Rejected::TimestampOutsideTolerance);
    }

    // `v1=<hex>`, possibly among others: the scheme leaves room for a second algorithm to be
    // added alongside the first, so a receiver takes the versions it knows and ignores the rest
    // rather than refusing a header that grew.
    let Some(offered) = signature
        .into_iter()
        .flat_map(|header| header.split(','))
        .find_map(|part| part.trim().strip_prefix("v1="))
    else {
        return Some(Rejected::MalformedSignature);
    };

    // The id is in the signed string, which is what makes the deduplication key below
    // trustworthy: an attacker cannot relabel a captured delivery to get past it.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts a key of any length");
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());

    let expected: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    if constant_time_eq(offered.as_bytes(), expected.as_bytes()) {
        None
    } else {
        Some(Rejected::SignatureMismatch)
    }
}

/// Compares in time that does not depend on how much of the signature was right.
///
/// `==` returns at the first difference, so how long the answer took reveals the length of the
/// matching prefix — enough, over enough requests, to construct a valid signature one byte at a
/// time without ever knowing the key. Written out rather than pulling in a crate for four lines,
/// the same way `business`'s token comparison is.
fn constant_time_eq(given: &[u8], expected: &[u8]) -> bool {
    given.len() == expected.len()
        && given
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or_default()
}
