//! Outbound notification webhooks.
//!
//! Best-effort fan-out, by design: each notification that passes the centre's
//! gates is POSTed to every configured webhook whose `kinds` accept it, with a
//! short timeout and a couple of retries — and **no queue**, ever. A webhook
//! that stays down drops its messages; the notification still lives in the
//! centre, which is the source of truth.

use std::sync::Arc;
use std::time::Duration;

use rucio_core::api::notifications::NotificationDto;
use tracing::{debug, warn};

use crate::config::{WebhookConfig, WebhookFormat};

/// Per-request timeout.
const TIMEOUT: Duration = Duration::from_secs(10);
/// Total attempts (1 try + 2 retries) with linear backoff.
const MAX_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// Fire every webhook that wants this notification. Returns immediately; each
/// delivery runs in its own task so a slow endpoint never blocks the notifier.
pub fn dispatch(client: &reqwest::Client, webhooks: &Arc<Vec<WebhookConfig>>, n: &NotificationDto) {
    if webhooks.is_empty() {
        return;
    }
    for (idx, wh) in webhooks.iter().enumerate() {
        // Per-webhook kind filter (empty = all).
        if !wh.kinds.is_empty() && !wh.kinds.contains(&n.kind) {
            continue;
        }
        let (body, content_type) = build_payload(wh, n);
        let client = client.clone();
        let url = wh.url.clone();
        let secret = wh.secret.clone();
        tokio::spawn(async move {
            send_with_retries(&client, &url, body, &content_type, secret.as_deref(), idx).await;
        });
    }
}

/// Build the request body and its Content-Type for a webhook.
fn build_payload(wh: &WebhookConfig, n: &NotificationDto) -> (String, String) {
    const JSON: &str = "application/json";
    match wh.format {
        WebhookFormat::Generic => (
            serde_json::to_string(n).unwrap_or_else(|_| "{}".to_string()),
            JSON.to_string(),
        ),
        WebhookFormat::Discord => (
            serde_json::json!({ "content": format!("**{}**\n{}", n.title, n.body) }).to_string(),
            JSON.to_string(),
        ),
        WebhookFormat::Slack => (
            serde_json::json!({ "text": format!("*{}*\n{}", n.title, n.body) }).to_string(),
            JSON.to_string(),
        ),
        WebhookFormat::Custom => {
            let content_type = wh.content_type.clone().unwrap_or_else(|| JSON.to_string());
            let json_escape = content_type.contains("json");
            let body = render_template(wh.template.as_deref().unwrap_or(""), n, json_escape);
            (body, content_type)
        }
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

/// POST the body to `url`, retrying a couple of times on transient failure.
async fn send_with_retries(
    client: &reqwest::Client,
    url: &str,
    body: String,
    content_type: &str,
    secret: Option<&str>,
    idx: usize,
) {
    let signature = secret.map(|s| format!("sha256={}", sign(s, body.as_bytes())));
    for attempt in 1..=MAX_ATTEMPTS {
        let mut req = client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .header(reqwest::header::USER_AGENT, "rucio")
            .timeout(TIMEOUT)
            .body(body.clone());
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
        let (body, ct) = build_payload(&wh(WebhookFormat::Discord, None), &dto("Done", "f.bin"));
        assert_eq!(ct, "application/json");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["content"], "**Done**\nf.bin");

        let (body, _) = build_payload(&wh(WebhookFormat::Slack, None), &dto("Done", "f.bin"));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["text"], "*Done*\nf.bin");
    }

    #[test]
    fn custom_template_escapes_into_valid_json() {
        // A title with quotes and a brace must not break a JSON template.
        let n = dto("He said \"hi\" {body}", "done");
        let (body, _) = build_payload(
            &wh(
                WebhookFormat::Custom,
                Some(r#"{"msg":"{title}","k":"{kind}"}"#),
            ),
            &n,
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        // The literal "{body}" inside the title is NOT expanded, and quotes are escaped.
        assert_eq!(v["msg"], "He said \"hi\" {body}");
        assert_eq!(v["k"], "download");
    }

    #[test]
    fn custom_plain_text_not_escaped() {
        let mut w = wh(WebhookFormat::Custom, Some("{title}: {body}"));
        w.content_type = Some("text/plain".to_string());
        let (body, ct) = build_payload(&w, &dto("Title", "Body"));
        assert_eq!(ct, "text/plain");
        assert_eq!(body, "Title: Body");
    }

    #[test]
    fn generic_is_the_dto() {
        let (body, _) = build_payload(&wh(WebhookFormat::Generic, None), &dto("T", "B"));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
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
