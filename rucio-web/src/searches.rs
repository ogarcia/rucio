use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::types::{
    ResultSource, SearchResult, SearchStartedResponse, SearchState, StartSearchRequest, format_size,
};

async fn api_start_search(keywords: Vec<String>) -> Option<u64> {
    let body = StartSearchRequest { keywords };
    gloo_net::http::Request::post("/api/v1/searches")
        .json(&body)
        .ok()?
        .send()
        .await
        .ok()?
        .json::<SearchStartedResponse>()
        .await
        .ok()
        .map(|r| r.id)
}

async fn api_start_download(link: String, provider: Option<String>) {
    let providers = provider.into_iter().collect::<Vec<_>>();
    let body = serde_json::json!({ "magnet": link, "providers": providers });
    let _ = gloo_net::http::Request::post("/api/v1/downloads")
        .json(&body)
        .ok()
        .map(|r| async move {
            let _ = r.send().await;
        });
}

#[component]
pub fn SearchesTab(
    results: RwSignal<Vec<SearchResult>>,
    searching: RwSignal<bool>,
    search_id: RwSignal<Option<u64>>,
) -> impl IntoView {
    let query = RwSignal::new(String::new());

    let do_search = move || {
        let raw = query.get();
        let kw: Vec<String> = raw
            .split_whitespace()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if kw.is_empty() {
            return;
        }
        results.set(vec![]);
        searching.set(true);
        search_id.set(None);

        spawn_local(async move {
            if let Some(id) = api_start_search(kw).await {
                search_id.set(Some(id));

                // Stop the "searching" indicator after 30 s if not already done.
                let captured_id = id;
                spawn_local(async move {
                    sleep(Duration::from_secs(30)).await;
                    if search_id.get() == Some(captured_id) {
                        searching.set(false);
                    }
                });
            } else {
                searching.set(false);
            }
        });
    };

    view! {
        <div class="tab-content">
            <div class="search-bar">
                <input
                    class="search-input"
                    type="text"
                    placeholder="Search for files…"
                    prop:value=move || query.get()
                    on:input=move |e| query.set(event_target_value(&e))
                    on:keydown=move |e| {
                        if e.key() == "Enter" { do_search(); }
                    }
                />
                <button
                    class="search-btn"
                    on:click=move |_| do_search()
                    disabled=move || searching.get()
                >
                    {move || if searching.get() { "Searching…" } else { "Search" }}
                </button>
            </div>

            {move || {
                let list = results.get();
                if list.is_empty() {
                    if searching.get() {
                        view! {
                            <div class="empty-state">
                                <p class="searching-indicator">"Searching…"</p>
                            </div>
                        }.into_any()
                    } else {
                        view! {
                            <div class="empty-state">
                                <p>"No results"</p>
                            </div>
                        }.into_any()
                    }
                } else {
                    view! {
                        <div class="results-header">
                            <span class="results-count">
                                {list.len().to_string()} " result(s)"
                                {move || if searching.get() {
                                    " — searching…"
                                } else { "" }}
                            </span>
                        </div>
                        <ul class="results-list">
                            {list.into_iter().map(|r| view! {
                                <ResultRow result=r/>
                            }).collect_view()}
                        </ul>
                    }.into_any()
                }
            }}
        </div>
    }
}

#[component]
fn ResultRow(result: SearchResult) -> impl IntoView {
    let link = result.download_link.clone();
    let provider = result.provider.clone();
    let can_download = link.is_some();

    let source_css = match result.source {
        ResultSource::Rucio => "source-badge source-rucio",
        ResultSource::Emule => "source-badge source-emule",
    };
    let source_label = match result.source {
        ResultSource::Rucio => "Rucio",
        ResultSource::Emule => "eMule",
    };

    view! {
        <li class="result-row">
            <span class="result-name">{result.name}</span>
            <span class="result-size">{format_size(result.size)}</span>
            <span class=source_css>{source_label}</span>
            {if can_download {
                view! {
                    <button class="btn-sm btn-primary" on:click=move |_| {
                        if let Some(l) = link.clone() {
                            let p = provider.clone();
                            spawn_local(async move {
                                api_start_download(l, p).await;
                            });
                        }
                    }>
                        "Download"
                    </button>
                }.into_any()
            } else {
                view! { <span class="result-no-link">"—"</span> }.into_any()
            }}
        </li>
    }
}
