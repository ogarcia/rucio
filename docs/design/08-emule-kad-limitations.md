# eMule / Kad2 network limitations

> This document lists behaviours that are **inherent to the eMule/Kad
> network**, not defects in Rucio. They cannot be fixed from our side
> because they stem from how the network and its other clients work. Keep
> them here so a known network quirk is not mistaken for — or reported as —
> a Rucio bug. Applies only with the `emule-compat` feature (see
> [07 — eMule / Kad2](07-emule-kad.md)).

## Accent-sensitive keyword search

On the eMule/Kad network, `camión` and `camion` are **different searches**
and return different results. There is no accent folding.

**Why.** eMule clients only *lowercase* a keyword before publishing it to the
DHT — they do **not** fold diacritics. The index is therefore keyed by the
lowercased word with its accents intact. If we folded accents on the query
side, the folded query (`camion`) would no longer match the key the network
actually stored (`camión`), so those entries would be missed entirely.

**How Rucio behaves.** We do **not** fold accents on Kad keyword queries — we
send the keyword lowercased but otherwise verbatim, so it matches what the
network indexed. This is asymmetric with native Rucio search, which *does*
fold accents (so `camion` and `camión` return the same Rucio results). The
practical advice for users — type the keyword with the same accents the file
actually uses — is in the [user search guide](../user/05-searching.md). The
normalization split is enforced by
`rucio_core::protocol::search::lowercase_keyword` (Kad) versus the folding
path used for native search.

## Unreliable keyword source counts

A Kad keyword result can report, say, **50 sources**, yet when you try to
download it **no peer actually has the file**. The source count attached to a
keyword hit is a hint, not a guarantee.

**What the number is.** It is the `FT_SOURCES` (tag `0x15`) value *reported by
the indexing node* — stored on the `KeywordHit.sources` field. It is a value
that **some client published**, cached by the index node until it expires; it
is **not** a live check that those sources are online right now.

**Why it lies (best current understanding).** This is genuinely murky — even
seasoned eMule users never get a crisp answer — but the plausible mechanisms,
which likely combine, are:

1. **Stale, TTL-cached data.** Publications carry a time-to-live and the index
   node keeps the last published value until it expires. Sources that have
   long gone offline are still counted until their record ages out.
2. **The keyword index and the source index are different node sets.** The
   count that rides along with a keyword hit lives on the nodes closest to the
   *keyword* hash and reflects popularity *at indexing time*. The actual
   sources live on the nodes closest to the *file* hash and require a separate
   source search — and even that returns *published* sources that may be
   offline.
3. **No proof of possession.** Any client can publish a keyword→file mapping
   (and a source count) **without actually holding the file**. Buggy or
   abusive clients inflate the number, and the network never verifies it.

**How Rucio behaves.** We treat the keyword source count as a popularity hint
only. Real availability is confirmed later, during the source search / download
phase (`FindingProviders`), where only peers that actually respond are counted.
So a result advertising many "sources" that then fails to find a real provider
is the network being unreliable, not Rucio losing the sources.

---

*This list grows as we confirm more inherited network quirks. Add an entry
only for behaviour that is genuinely the network's (or another client's), not
ours — Rucio's own limitations and TODOs belong elsewhere.*
