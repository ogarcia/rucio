use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::SearchStore;
use crate::icons::{self, Icon};
use crate::types::{
    ResultSource, SearchDetailResponse, SearchResult, SearchStartedResponse, SearchState,
    SearchSummary, StartSearchRequest, format_size,
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

async fn api_list_searches() -> Option<Vec<SearchSummary>> {
    gloo_net::http::Request::get("/api/v1/searches")
        .send()
        .await
        .ok()?
        .json::<crate::types::SearchListResponse>()
        .await
        .ok()
        .map(|r| r.searches)
}

async fn api_search_detail(id: u64) -> Option<SearchDetailResponse> {
    gloo_net::http::Request::get(&format!("/api/v1/searches/{id}"))
        .send()
        .await
        .ok()?
        .json::<SearchDetailResponse>()
        .await
        .ok()
}

async fn api_relaunch(id: u64) -> Option<u64> {
    gloo_net::http::Request::post(&format!("/api/v1/searches/{id}/relaunch"))
        .send()
        .await
        .ok()?
        .json::<SearchStartedResponse>()
        .await
        .ok()
        .map(|r| r.id)
}

async fn api_delete_search(id: u64) {
    let _ = gloo_net::http::Request::delete(&format!("/api/v1/searches/{id}"))
        .send()
        .await;
}

async fn api_start_download(link: String, provider: Option<String>) {
    let providers = provider.into_iter().collect::<Vec<_>>();
    let body = serde_json::json!({ "magnet": link, "providers": providers });
    if let Ok(req) = gloo_net::http::Request::post("/api/v1/downloads").json(&body) {
        let _ = req.send().await;
    }
}

fn state_label(s: &SearchState) -> &'static str {
    match s {
        SearchState::Running => "running",
        SearchState::Done => "done",
        SearchState::Cancelled => "cancelled",
    }
}

/// Fetch a search's full result set once (the snapshot of what's accumulated so
/// far, Rucio + eMule) and merge it into the store; live additions arrive via
/// the WebSocket. Also syncs the summary's state/count.
fn load_detail(search: SearchStore, id: u64) {
    spawn_local(async move {
        if let Some(d) = api_search_detail(id).await {
            let count = d.results.len();
            let st = d.state.clone();
            search.results.update(|m| {
                let v = m.entry(id).or_default();
                for r in d.results {
                    if !v.iter().any(|x| x.result_id == r.result_id) {
                        v.push(r);
                    }
                }
            });
            search.list.update(|list| {
                if let Some(s) = list.iter_mut().find(|s| s.id == id) {
                    s.state = st;
                    s.result_count = count;
                }
            });
        }
    });
}

fn refresh_list(search: SearchStore) {
    spawn_local(async move {
        if let Some(l) = api_list_searches().await {
            search.list.set(l);
        }
    });
}

#[component]
pub fn SearchesTab(search: SearchStore) -> impl IntoView {
    let query = RwSignal::new(String::new());

    // Load the recent-search list once when the tab mounts; the WS keeps it live.
    Effect::new(move |_| {
        refresh_list(search);
    });

    let do_search = move || {
        let kw: Vec<String> = query
            .get()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        if kw.is_empty() {
            return;
        }
        spawn_local(async move {
            if let Some(id) = api_start_search(kw).await {
                if let Some(l) = api_list_searches().await {
                    search.list.set(l);
                }
                search.selected.set(Some(id));
                load_detail(search, id);
            }
        });
    };

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
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
                <button class="search-btn" on:click=move |_| do_search()>
                    "Search"
                </button>
            </div>
            </div>

            // ── Recent searches ───────────────────────────────────────────
            <Show when=move || !search.list.get().is_empty() fallback=|| ()>
                <div class="search-history">
                    <select
                        class="search-history-select"
                        prop:value=move || search.selected.get().map(|i| i.to_string()).unwrap_or_default()
                        on:change=move |e| {
                            if let Ok(id) = event_target_value(&e).parse::<u64>() {
                                search.selected.set(Some(id));
                                load_detail(search, id);
                            }
                        }
                    >
                        <option value="" disabled=true>"Recent searches…"</option>
                        {move || search.list.get().into_iter().map(|s| {
                            let label = format!(
                                "{} — {} ({})",
                                s.keywords.join(" "),
                                state_label(&s.state),
                                s.result_count,
                            );
                            view! { <option value=s.id.to_string()>{label}</option> }
                        }).collect_view()}
                    </select>
                    <button
                        class="icon-btn"
                        title="Relaunch search"
                        disabled=move || search.selected.get().is_none()
                        on:click=move |_| {
                            if let Some(old) = search.selected.get_untracked() {
                                spawn_local(async move {
                                    if let Some(new_id) = api_relaunch(old).await {
                                        if let Some(l) = api_list_searches().await {
                                            search.list.set(l);
                                        }
                                        search.selected.set(Some(new_id));
                                        load_detail(search, new_id);
                                    }
                                });
                            }
                        }
                    >
                        <Icon paths=icons::REFRESH/>
                    </button>
                    <button
                        class="icon-btn icon-btn-danger"
                        title="Delete search"
                        disabled=move || search.selected.get().is_none()
                        on:click=move |_| {
                            if let Some(id) = search.selected.get_untracked() {
                                spawn_local(async move {
                                    api_delete_search(id).await;
                                    if let Some(l) = api_list_searches().await {
                                        search.list.set(l);
                                    }
                                    search.results.update(|m| { m.remove(&id); });
                                    search.selected.set(None);
                                });
                            }
                        }
                    >
                        <Icon paths=icons::TRASH/>
                    </button>
                </div>
            </Show>

            <div class="tab-scroll">
            {move || {
                let sel = search.selected.get();
                let results = sel
                    .and_then(|id| search.results.with(|m| m.get(&id).cloned()))
                    .unwrap_or_default();
                let state = sel.and_then(|id| {
                    search.list.with(|l| l.iter().find(|s| s.id == id).map(|s| s.state.clone()))
                });
                if sel.is_none() {
                    view! {
                        <div class="empty-state"><p>"Search for files, or pick a recent search"</p></div>
                    }.into_any()
                } else if results.is_empty() {
                    let running = state == Some(SearchState::Running);
                    if running {
                        view! { <div class="empty-state"><p class="searching-indicator">"Searching…"</p></div> }.into_any()
                    } else {
                        view! { <div class="empty-state"><p>"No results"</p></div> }.into_any()
                    }
                } else {
                    view! {
                        <div class="results-header">
                            <span class="results-count">
                                {results.len().to_string()} " result(s)"
                                {if state == Some(SearchState::Running) { " — searching…" } else { "" }}
                            </span>
                        </div>
                        <ul class="results-list">
                            <For
                                each=move || {
                                    search.selected.get()
                                        .and_then(|id| search.results.with(|m| m.get(&id).cloned()))
                                        .unwrap_or_default()
                                }
                                key=|r| r.result_id
                                children=|r| view! { <ResultRow result=r/> }
                            />
                        </ul>
                    }.into_any()
                }
            }}
            </div>
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
