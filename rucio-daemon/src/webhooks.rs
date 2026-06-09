//! Outbound notification webhooks.
//!
//! Best-effort fan-out, by design: each notification that passes the centre's
//! gates is POSTed to every configured webhook whose `kinds` accept it, with a
//! short timeout and a couple of retries — and **no queue**, ever. A webhook
//! that stays down drops its messages; the notification still lives in the
//! centre, which is the source of truth.

use std::time::Duration;

use rucio_core::api::notifications::NotificationDto;
use tracing::{debug, warn};

use crate::config::{WebhookConfig, WebhookFormat};

/// Per-request timeout.
const TIMEOUT: Duration = Duration::from_secs(10);
/// Total attempts (1 try + 2 retries) with linear backoff.
const MAX_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

const JSON: &str = "application/json";

/// A fully-built outbound request: where to POST, what, and any extra headers
/// (e.g. ntfy's `Title`). The URL can differ from `wh.url` — Telegram strips the
/// chat id out of the query into the body.
struct Prepared {
    url: String,
    body: String,
    content_type: String,
    headers: Vec<(&'static str, String)>,
}

/// Fire every webhook that wants this notification. Returns immediately; each
/// delivery runs in its own task so a slow endpoint never blocks the notifier.
pub fn dispatch(client: &reqwest::Client, webhooks: &[WebhookConfig], n: &NotificationDto) {
    if webhooks.is_empty() {
        return;
    }
    for (idx, wh) in webhooks.iter().enumerate() {
        // Per-webhook kind filter (empty = all).
        if !wh.kinds.is_empty() && !wh.kinds.contains(&n.kind) {
            continue;
        }
        let prepared = build_payload(wh, n);
        let client = client.clone();
        let secret = wh.secret.clone();
        tokio::spawn(async move {
            send_with_retries(&client, prepared, secret.as_deref(), idx).await;
        });
    }
}

/// Build the outbound request (URL, body, Content-Type, extra headers) for a
/// webhook from a notification.
fn build_payload(wh: &WebhookConfig, n: &NotificationDto) -> Prepared {
    let json = |url: String, body: String| Prepared {
        url,
        body,
        content_type: JSON.to_string(),
        headers: vec![],
    };
    match wh.format {
        WebhookFormat::Generic => json(
            wh.url.clone(),
            serde_json::to_string(n).unwrap_or_else(|_| "{}".to_string()),
        ),
        WebhookFormat::Discord => json(
            wh.url.clone(),
            serde_json::json!({ "content": format!("**{}**\n{}", n.title, n.body) }).to_string(),
        ),
        WebhookFormat::Slack => json(
            wh.url.clone(),
            serde_json::json!({ "text": format!("*{}*\n{}", n.title, n.body) }).to_string(),
        ),
        WebhookFormat::Telegram => {
            // chat_id rides in the URL query; move it into the JSON body since
            // Telegram doesn't combine query params with a JSON body.
            let (base, chat_id) = split_telegram_url(&wh.url);
            let text = format!("{}\n{}", n.title, n.body);
            json(
                base,
                serde_json::json!({ "chat_id": chat_id, "text": text }).to_string(),
            )
        }
        WebhookFormat::Ntfy => Prepared {
            url: wh.url.clone(),
            body: n.body.clone(),
            content_type: "text/plain; charset=utf-8".to_string(),
            // Strip newlines: a header value must be single-line.
            headers: vec![("Title", n.title.replace(['\r', '\n'], " "))],
        },
        WebhookFormat::Custom => {
            let content_type = wh.content_type.clone().unwrap_or_else(|| JSON.to_string());
            let json_escape = content_type.contains("json");
            let body = render_template(wh.template.as_deref().unwrap_or(""), n, json_escape);
            Prepared {
                url: wh.url.clone(),
                body,
                content_type,
                headers: vec![],
            }
        }
    }
}

/// Split a Telegram webhook URL into `(endpoint, chat_id)`, pulling `chat_id`
/// out of the query string (URL-decoded). The chat id is empty if absent — the
/// request will then be rejected by Telegram, surfacing the misconfiguration.
fn split_telegram_url(url: &str) -> (String, String) {
    match url.split_once('?') {
        Some((base, query)) => {
            let chat_id = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("chat_id="))
                .map(|v| {
                    urlencoding::decode(v)
                        .map(|c| c.into_owned())
                        .unwrap_or_else(|_| v.to_string())
                })
                .unwrap_or_default();
            (base.to_string(), chat_id)
        }
        None => (url.to_string(), String::new()),
    }
}

/// Render a custom template, replacing only the exact known tokens in a single
/// left-to-right pass. A `{` that doesn't begin a known token (e.g. a JSON
/// object brace) is copied verbatim, and inserted values are never re-scanned —
/// so neither JSON structure nor a value containing `{body}` can be mangled.
fn render_template(tpl: &str, n: &NotificationDto, json_escape: bool) -> String {
    let esc = |s: &str| -> String {
        if !json_escape {
            return s.to_string();
        }
        // JSON-escape, then drop the surrounding quotes serde_json adds — the
        // template supplies the quotes.
        let quoted = serde_json::Value::String(s.to_string()).to_string();
        quoted[1..quoted.len() - 1].to_string()
    };
    let ref_val = n.ref_key.as_deref().unwrap_or("");
    let tokens: [(&str, String); 6] = [
        ("{title}", esc(&n.title)),
        ("{body}", esc(&n.body)),
        ("{kind}", n.kind.as_str().to_string()),
        ("{ref}", esc(ref_val)),
        ("{id}", n.id.to_string()),
        ("{created_at}", n.created_at.to_string()),
    ];

    let mut out = String::with_capacity(tpl.len());
    let mut rest = tpl;
    while let Some(pos) = rest.find('{') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos..];
        match tokens.iter().find(|(tok, _)| after.starts_with(tok)) {
            Some((tok, val)) => {
                out.push_str(val);
                rest = &after[tok.len()..];
            }
            None => {
                out.push('{');
                rest = &after[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// HMAC-SHA256 of `body` keyed by `secret`, hex-encoded.
fn sign(secret: &str, body: &[u8]) -> String {
    use hmac::Mac;
    use hmac::digest::KeyInit;
    use sha2::Sha256;
    let mut mac = <hmac::Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// POST the prepared request, retrying a couple of times on transient failure.
async fn send_with_retries(
    client: &reqwest::Client,
    prepared: Prepared,
    secret: Option<&str>,
    idx: usize,
) {
    let Prepared {
        url,
        body,
        content_type,
        headers,
    } = prepared;
    let signature = secret.map(|s| format!("sha256={}", sign(s, body.as_bytes())));
    for attempt in 1..=MAX_ATTEMPTS {
        let mut req = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, &content_type)
            .header(reqwest::header::USER_AGENT, "rucio")
            .timeout(TIMEOUT)
            .body(body.clone());
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        if let Some(sig) = &signature {
            req = req.header("X-Rucio-Signature", sig);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!(webhook = idx, status = %resp.status(), "webhook delivered");
                return;
            }
            Ok(resp) => warn!(webhook = idx, status = %resp.status(), attempt, "webhook rejected"),
            Err(e) => warn!(webhook = idx, attempt, "webhook send failed: {e}"),
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(RETRY_BACKOFF).await;
        }
    }
    warn!(
        webhook = idx,
        "webhook gave up after {MAX_ATTEMPTS} attempts"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rucio_core::api::notifications::NotificationKind;

    fn dto(title: &str, body: &str) -> NotificationDto {
        NotificationDto {
            id: 7,
            kind: NotificationKind::Download,
            title: title.to_string(),
            body: body.to_string(),
            ref_key: Some("abcd".to_string()),
            created_at: 1700,
            read: false,
        }
    }

    fn wh(format: WebhookFormat, template: Option<&str>) -> WebhookConfig {
        WebhookConfig {
            url: "http://x".to_string(),
            format,
            kinds: vec![],
            secret: None,
            template: template.map(String::from),
            content_type: None,
        }
    }

    #[test]
    fn discord_and_slack_presets() {
        let p = build_payload(&wh(WebhookFormat::Discord, None), &dto("Done", "f.bin"));
        assert_eq!(p.content_type, "application/json");
        let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
        assert_eq!(v["content"], "**Done**\nf.bin");

        let p = build_payload(&wh(WebhookFormat::Slack, None), &dto("Done", "f.bin"));
        let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
        assert_eq!(v["text"], "*Done*\nf.bin");
    }

    #[test]
    fn ntfy_uses_plain_body_and_title_header() {
        let mut w = wh(WebhookFormat::Ntfy, None);
        w.url = "https://ntfy.sh/mytopic".to_string();
        let p = build_payload(&w, &dto("Download complete", "movie.mkv"));
        assert_eq!(p.url, "https://ntfy.sh/mytopic");
        assert!(p.content_type.starts_with("text/plain"));
        assert_eq!(p.body, "movie.mkv");
        assert_eq!(p.headers, vec![("Title", "Download complete".to_string())]);
    }

    #[test]
    fn telegram_moves_chat_id_from_query_to_body() {
        let mut w = wh(WebhookFormat::Telegram, None);
        w.url = "https://api.telegram.org/bot123:ABC/sendMessage?chat_id=98765".to_string();
        let p = build_payload(&w, &dto("Done", "f.bin"));
        // Query stripped from the URL; chat_id + text in the JSON body.
        assert_eq!(p.url, "https://api.telegram.org/bot123:ABC/sendMessage");
        let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
        assert_eq!(v["chat_id"], "98765");
        assert_eq!(v["text"], "Done\nf.bin");
    }

    #[test]
    fn custom_template_escapes_into_valid_json() {
        // A title with quotes and a brace must not break a JSON template.
        let n = dto("He said \"hi\" {body}", "done");
        let p = build_payload(
            &wh(
                WebhookFormat::Custom,
                Some(r#"{"msg":"{title}","k":"{kind}"}"#),
            ),
            &n,
        );
        let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
        // The literal "{body}" inside the title is NOT expanded, and quotes are escaped.
        assert_eq!(v["msg"], "He said \"hi\" {body}");
        assert_eq!(v["k"], "download");
    }

    #[test]
    fn custom_plain_text_not_escaped() {
        let mut w = wh(WebhookFormat::Custom, Some("{title}: {body}"));
        w.content_type = Some("text/plain".to_string());
        let p = build_payload(&w, &dto("Title", "Body"));
        assert_eq!(p.content_type, "text/plain");
        assert_eq!(p.body, "Title: Body");
    }

    #[test]
    fn generic_is_the_dto() {
        let p = build_payload(&wh(WebhookFormat::Generic, None), &dto("T", "B"));
        let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
        assert_eq!(v["title"], "T");
        assert_eq!(v["kind"], "download");
    }

    #[test]
    fn signature_is_stable_hex() {
        let s = sign("secret", b"payload");
        assert_eq!(s.len(), 64);
        assert_eq!(s, sign("secret", b"payload"));
        assert_ne!(s, sign("other", b"payload"));
    }
}
