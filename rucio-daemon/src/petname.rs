//! Podman-style default nickname generator for the eMule identity.
//!
//! Produces names like `"Awesome Magical Rucio"` — two distinct positive
//! adjectives followed by `"Rucio"`. It exists so that users who never pick
//! their own eMule nickname don't all show up as an identical `"rucio"` in
//! peers' transfer lists. The name is generated once on first run, persisted to
//! the config file, and never regenerated (a user-chosen nick always wins).
//!
//! Deliberately English-only and **not** translatable: it's a cosmetic handle,
//! and the recognisable `"… Rucio"` suffix is the whole point.

/// Positive, friendly adjectives. Kept wholesome and unambiguous; the list is
/// large enough that collisions between two random users are unlikely.
const ADJECTIVES: &[&str] = &[
    "Amazing",
    "Astonishing",
    "Awesome",
    "Blissful",
    "Bold",
    "Brave",
    "Bright",
    "Brilliant",
    "Calm",
    "Charming",
    "Cheerful",
    "Clever",
    "Cosmic",
    "Courageous",
    "Curious",
    "Daring",
    "Dazzling",
    "Delightful",
    "Eager",
    "Elegant",
    "Fabulous",
    "Fantastic",
    "Fearless",
    "Gentle",
    "Gifted",
    "Glorious",
    "Graceful",
    "Happy",
    "Heroic",
    "Incredible",
    "Jolly",
    "Joyful",
    "Keen",
    "Kind",
    "Legendary",
    "Lively",
    "Lucky",
    "Magical",
    "Majestic",
    "Marvelous",
    "Mighty",
    "Noble",
    "Optimistic",
    "Peaceful",
    "Playful",
    "Prodigious",
    "Radiant",
    "Serene",
    "Shiny",
    "Sparkling",
    "Splendid",
    "Stellar",
    "Sublime",
    "Swift",
    "Thin",
    "Unbelievable",
    "Valiant",
    "Vibrant",
    "Vivid",
    "Witty",
    "Wonderful",
    "Zealous",
    "Zesty",
];

/// Generate a random `"<Adjective> <Adjective> Rucio"` nickname, with the two
/// adjectives guaranteed to differ.
pub fn random_nick() -> String {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    let n = ADJECTIVES.len();
    let a = (u16::from_le_bytes([bytes[0], bytes[1]]) as usize) % n;
    let mut b = (u16::from_le_bytes([bytes[2], bytes[3]]) as usize) % n;
    if b == a {
        b = (b + 1) % n;
    }
    format!("{} {} Rucio", ADJECTIVES[a], ADJECTIVES[b])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nick_has_two_distinct_adjectives_and_suffix() {
        for _ in 0..1000 {
            let nick = random_nick();
            let parts: Vec<&str> = nick.split(' ').collect();
            assert_eq!(parts.len(), 3, "unexpected shape: {nick}");
            assert_eq!(parts[2], "Rucio");
            assert!(ADJECTIVES.contains(&parts[0]));
            assert!(ADJECTIVES.contains(&parts[1]));
            assert_ne!(parts[0], parts[1], "adjectives must differ: {nick}");
        }
    }
}
