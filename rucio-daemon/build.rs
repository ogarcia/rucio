// Ensure rucio-web/dist/ exists so that rust-embed (web-ui feature) can
// compile even when trunk has not been run yet.  The directory may be empty
// (no assets embedded) — the daemon will then return 404 for every web
// request, which is acceptable for CI and development builds without the
// frontend.
fn main() {
    let dist = std::path::Path::new("../rucio-web/dist");
    if !dist.exists() {
        std::fs::create_dir_all(dist).expect("failed to create rucio-web/dist");
    }
}
