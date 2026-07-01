# Rucio — Flatpak (own bundle)

This packages the Rucio desktop app (the Tauri shell that embeds the daemon and
its web UI) as a Flatpak for personal / self-hosted use. It is an **own bundle**,
not a Flathub submission: the manifest packages a pre-built release binary
instead of building from source in a sandbox, so no crate vendoring is required.

App ID: `me.ogarcia.rucio`. All configuration and the database live inside the
sandbox at `~/.var/app/me.ogarcia.rucio/`.

## Prerequisites

- A Rust toolchain and the `wasm32-unknown-unknown` target.
- [`trunk`](https://trunkrs.dev/) for the web UI.
- WebKitGTK development libraries, to link the Tauri shell (e.g. `webkit2gtk-4.1`).
- `flatpak`, `flatpak-builder`, and the GNOME runtime/SDK:

  ```sh
  flatpak install flathub org.gnome.Platform//48 org.gnome.Sdk//48
  ```

## Build & install

From the repository root:

```sh
rucio-tauri/flatpak/build.sh
flatpak-builder --user --install --force-clean build-dir \
    rucio-tauri/flatpak/me.ogarcia.rucio.yml
flatpak run me.ogarcia.rucio
```

`build.sh` builds the web UI with `trunk` and then compiles the release binary
at `target/release/rucio-tauri`, which the manifest packages.

## Filesystem access

By design the sandbox is tight: the app can only read and write your **Downloads**
folder (`--filesystem=xdg-download`), which is where it downloads to and shares
from by default. Its own config and database never leave the sandbox.

To share files from other directories, widen the sandbox yourself — nothing
broader is baked into the bundle:

```sh
flatpak override --user --filesystem=~/Music me.ogarcia.rucio
# or grant the whole home dir:
flatpak override --user --filesystem=home me.ogarcia.rucio
```

[Flatseal](https://flathub.org/apps/com.github.tchx84.Flatseal) offers the same
control with a GUI.

## Notes / limitations

- **No system tray on Linux.** GNOME ships no tray, and the tray backend needs
  libappindicator (absent in the GNOME runtime), so the app runs without one.
  Instead, closing the window hides it and the app keeps running (and sharing)
  in the background via the XDG Background portal — the first time, the desktop
  asks for consent. Reopen it by launching Rucio again (a second launch
  re-shows the running instance), and quit it from GNOME's **Background Apps**
  menu. On desktops without a portal it still runs; it just appears as an
  ordinary background process.
- **Autostart at login** is opt-in via a hand-edited setting (there is no in-app
  toggle, as the web UI does not use Tauri IPC). On first run the app seeds
  `desktop.toml` next to the daemon's config — inside the Flatpak that is
  `~/.var/app/me.ogarcia.rucio/config/rucio/desktop.toml` — with
  `autostart = false`. Flip it to `true`:

  ```toml
  autostart = true
  ```

  On the next launch Rucio asks the Background portal to add the host-side
  autostart entry. This is a desktop-shell setting only; it never touches the
  daemon, CLI or web UI.
- The runtime version is pinned in the manifest (`runtime-version`); bump it as
  newer GNOME runtimes ship.
