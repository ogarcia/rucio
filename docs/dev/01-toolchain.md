# Toolchain

## Rust

Rust **1.85 or later** is required (the codebase uses the 2024 edition).

### Arch Linux

```sh
sudo pacman -S rust
```

The Arch package ships a recent stable compiler.  No `rustup` is needed.

### Other Linux / macOS / Windows (WSL2)

Install via [rustup](https://rustup.rs/):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

The default stable toolchain is sufficient.  After installation:

```sh
rustc --version   # should print 1.85.0 or later
```

---

## WASM target (`wasm32-unknown-unknown`)

Only needed to build the [`rucio-web`](03-web-ui.md) frontend.

### Arch Linux (system Rust, no rustup)

```sh
sudo pacman -S rust-wasm
```

### rustup-managed toolchain

```sh
rustup target add wasm32-unknown-unknown
```

Verify:

```sh
rustc --print sysroot | xargs -I{} ls {}/lib/rustlib/ | grep wasm32
```

---

## trunk

Only needed to build the [`rucio-web`](03-web-ui.md) frontend.  trunk is the
build tool that compiles the Leptos crate to WASM and produces the static
assets consumed by the daemon's `web-ui` feature.

### Arch Linux

```sh
sudo pacman -S trunk
```

### Other platforms

```sh
cargo install trunk
```

Or download a pre-built binary from the
[trunk releases page](https://github.com/trunk-rs/trunk/releases).

Verify:

```sh
trunk --version
```

---

## Optional: `wasm-opt`

trunk can run `wasm-opt` (from the `binaryen` suite) to shrink the WASM
bundle when building in release mode.  It is optional — the build works
without it; the `.wasm` file will just be larger.

### Arch Linux

```sh
sudo pacman -S binaryen
```

### Other platforms

```sh
# Ubuntu / Debian
sudo apt install binaryen

# macOS (Homebrew)
brew install binaryen
```

To enable optimisation, pass `data-wasm-opt="z"` in `rucio-web/index.html`:

```html
<link data-trunk rel="rust" data-wasm-opt="z"/>
```
