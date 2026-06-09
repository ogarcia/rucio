# Notifications

Rucio keeps a small notification centre — the bell in the top-right corner of
the web UI. Click it to open a panel with recent events; a badge shows how many
are unread. Two kinds of event are produced:

- **Download** — a download finished (Rucio or eMule).
- **System** — a background event, e.g. indexing your shared files completed.

## Turning notifications on or off

Open **Settings → Notifications**. The first switch is the master on/off; below
it, one switch per kind lets you keep, say, download notifications while
silencing system ones. Changes apply immediately. Turning the master switch off
also hides the bell from the top bar — nothing is produced, so there's nothing
to show.

## Webhooks

A webhook forwards every notification you receive to an external service — so
you can get a Discord ping, a Telegram message or a phone push when a download
finishes, even when the web UI isn't open.

Add one in **Settings → Notifications → Add webhook**. Each webhook has:

- **Format** — the service/shape (see below).
- **URL** — where to send it (what goes here depends on the format).
- **Kinds** — tick which kinds to forward. Tick none to forward all.
- **Secret** (optional) — if set, the request body is signed with HMAC-SHA256
  and sent in the `X-Rucio-Signature: sha256=<hex>` header, so your receiver can
  verify it really came from your daemon (see
  [Verifying the signature](#verifying-the-signature)).

Webhooks are saved together with the rest of the settings when you click
**Save** at the bottom of the dialog — there's no separate button. Delivery is
best-effort (a short retry, then it gives up); the event always stays in the
notification centre regardless.

Use the **Test** button on a row to send a sample notification right away and
see whether it was delivered — handy for checking the URL and any secret before
you rely on it. You don't need to save first; it tests the row as currently
filled in.

What to put in each field, by format:

### Discord

1. In Discord: **Server Settings → Integrations → Webhooks → New Webhook**, then
   **Copy Webhook URL**.
2. **Format:** Discord. **URL:** paste that webhook URL. Leave the rest empty.

### Slack

1. Create an **Incoming Webhook** for your workspace
   (<https://api.slack.com/messaging/webhooks>) and copy its URL.
2. **Format:** Slack. **URL:** paste the Slack webhook URL.

### Telegram

You need a bot token and a chat id.

1. In Telegram, message **@BotFather**, send `/newbot`, and copy the **token**
   it gives you (looks like `123456:ABC-DEF…`).
2. Send any message to your new bot, then open
   `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and read the
   `"chat":{"id":…}` value — that's your **chat id**. (Or message
   **@userinfobot**, which replies with your id.)
3. **Format:** Telegram. **URL:**
   `https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<CHAT_ID>`
   — token in the path, chat id in the query. Rucio moves the chat id into the
   request body for you.

### ntfy

1. Pick a **topic** name — treat it like a password, since anyone who knows it
   can read your notifications on the public server (or self-host ntfy). E.g.
   `rucio-alice-7Qx9`.
2. **Format:** ntfy. **URL:** `https://ntfy.sh/<topic>` (or your own instance).
3. Subscribe to the same topic in the ntfy app (Android/iOS) or at
   `https://ntfy.sh/<topic>` in a browser to receive the pushes.

### Generic

A plain `POST` of the notification as JSON — for your own endpoint.

- **Format:** Generic. **URL:** your endpoint.
- The body is:

  ```json
  {
    "id": 42,
    "kind": "download",
    "title": "Download complete",
    "body": "ubuntu-24.04.iso",
    "ref_key": "9f86d0818…",
    "created_at": 1781000861,
    "read": false
  }
  ```

### Custom

For any service without a preset: you write the request body yourself.

- **Format:** Custom. **URL:** your endpoint.
- **Template:** the request body, with these placeholders substituted:
  `{title}`, `{body}`, `{kind}`, `{ref}`, `{id}`, `{created_at}`. For a JSON
  template the values are escaped for you, so quotes in a title won't break it.
- **Content-Type:** defaults to `application/json`; change it if your service
  expects something else (e.g. `text/plain`).

Example template for a JSON endpoint:

```json
{"event":"{kind}","message":"{title} — {body}"}
```

## Verifying the signature

Only relevant when the receiver is your own service (the **Generic** or
**Custom** formats) — third-party presets like Discord or ntfy ignore the
header. When you set a **Secret**, Rucio computes
`HMAC-SHA256(secret, request_body)` and sends it hex-encoded as:

```
X-Rucio-Signature: sha256=3b8e...c1
```

To verify, recompute the HMAC over the **raw request body** (the exact bytes you
received, before any JSON parsing or re-formatting — re-serializing can change
bytes and break the match) with the same secret, and compare. Three things
matter: use the raw body, keep the `sha256=` prefix, and compare in constant
time.

A minimal receiver in Python (Flask):

```python
import hmac, hashlib
from flask import Flask, request, abort

SECRET = b"the-secret-you-set-on-the-webhook"
app = Flask(__name__)

@app.post("/rucio-hook")
def hook():
    body = request.get_data()  # raw bytes, do NOT use request.json here
    sent = request.headers.get("X-Rucio-Signature", "")
    expected = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sent, expected):
        abort(401)  # forged or misconfigured — reject
    # genuine: now it's safe to parse and act on `body`
    return "", 204
```

To eyeball it from a shell, this prints the hex digest Rucio would send (prepend
`sha256=`):

```sh
printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET"
```

The secret never travels with the request — only the signature does — so a
receiver that doesn't know the secret can't forge a valid one.

## Editing `config.toml` directly

Webhooks are stored under `[[notifications.webhooks]]` in `config.toml`, so you
can also edit them by hand instead of using the UI:

```toml
[notifications]
enabled   = true
downloads = true
system    = true

[[notifications.webhooks]]
format = "telegram"
url    = "https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<CHAT_ID>"
kinds  = ["download"]      # omit or leave empty for all kinds

[[notifications.webhooks]]
format = "ntfy"
url    = "https://ntfy.sh/rucio-alice-7Qx9"
```
