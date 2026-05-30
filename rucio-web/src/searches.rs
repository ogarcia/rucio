use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::SearchStore;
use crate::downloads::refresh_downloads;
use crate::icons::{self, Icon};
use crate::types::{
    DownloadResponse, DownloadState, ResultSource, SearchDetailResponse, SearchResult,
    SearchStartedResponse, SearchState, SearchSummary, StartSearchRequest, format_size,
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

/// Outcome of requesting a download for a result, mapped from the HTTP status.
#[derive(Clone, Copy, PartialEq)]
enum DlOutcome {
    /// 202 — newly queued, or already in progress (idempotent).
    Queued,
    /// 409 — already completed or already shared locally.
    AlreadyHave,
    /// 400/500/network failure.
    Error,
}

/// Start a download for a result link. eMule (`ed2k://`) links go to the eMule
/// endpoint; everything else is treated as a Rucio magnet. The HTTP status is
/// mapped to a [`DlOutcome`] so the row can show real feedback (e.g. the 409
/// "already have it" case the backend returns for completed/shared content).
async fn api_start_download(link: String, provider: Option<String>) -> DlOutcome {
    let builder = if link.starts_with("ed2k://") {
        let body = serde_json::json!({ "link": link });
        gloo_net::http::Request::post("/api/v1/downloads/ed2k").json(&body)
    } else {
        let providers = provider.into_iter().collect::<Vec<_>>();
        let body = serde_json::json!({ "magnet": link, "providers": providers });
        gloo_net::http::Request::post("/api/v1/downloads").json(&body)
    };
    let Ok(req) = builder else {
        return DlOutcome::Error;
    };
    match req.send().await {
        Ok(r) => match r.status() {
            202 => DlOutcome::Queued,
            409 => DlOutcome::AlreadyHave,
            _ => DlOutcome::Error,
        },
        Err(_) => DlOutcome::Error,
    }
}

fn state_label(s: &SearchState) -> &'static str {
    match s {
        SearchState::Running => "running",
        SearchState::Done => "done",
        SearchState::Cancelled => "cancelled",
    }
}

// ── Result filtering & sorting ──────────────────────────────────────────────

/// Source filter applied to the visible result list.
#[derive(Clone, Copy, PartialEq)]
enum SourceFilter {
    All,
    Rucio,
    Emule,
}

impl SourceFilter {
    fn matches(self, src: &ResultSource) -> bool {
        match self {
            SourceFilter::All => true,
            SourceFilter::Rucio => *src == ResultSource::Rucio,
            SourceFilter::Emule => *src == ResultSource::Emule,
        }
    }
}

/// Sort order applied to the visible result list.
#[derive(Clone, Copy, PartialEq)]
enum SortBy {
    NameAsc,
    NameDesc,
    SizeDesc,
    SizeAsc,
}

impl SortBy {
    fn apply(self, v: &mut [SearchResult]) {
        use std::cmp::Reverse;
        match self {
            SortBy::NameAsc => v.sort_by_key(|r| r.name.to_lowercase()),
            SortBy::NameDesc => v.sort_by_key(|r| Reverse(r.name.to_lowercase())),
            SortBy::SizeDesc => v.sort_by_key(|r| Reverse(r.size)),
            SortBy::SizeAsc => v.sort_by_key(|r| r.size),
        }
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
            let queued = d.emule_queued;
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
                    s.emule_queued = queued;
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
pub fn SearchesTab(
    search: SearchStore,
    downloads: RwSignal<Vec<DownloadResponse>>,
) -> impl IntoView {
    let query = RwSignal::new(String::new());

    // Result filter/sort controls (apply to the currently selected search).
    let filter_source: RwSignal<SourceFilter> = RwSignal::new(SourceFilter::All);
    let filter_text: RwSignal<String> = RwSignal::new(String::new());
    let sort_by: RwSignal<SortBy> = RwSignal::new(SortBy::SizeDesc);

    // Recent-search dropdown open/closed. A custom dropdown (rather than a
    // native <select>) is used because Firefox/Linux draws the native option
    // highlight at the content's intrinsic width, not the stretched control's
    // width, leaving the right part of each row unhighlighted on wide screens.
    let dd_open: RwSignal<bool> = RwSignal::new(false);

    // Label shown on the dropdown button: the selected search, or a placeholder.
    let current_label = move || {
        search
            .selected
            .get()
            .and_then(|id| {
                search.list.with(|l| {
                    l.iter().find(|s| s.id == id).map(|s| {
                        format!(
                            "{} — {} ({})",
                            s.keywords.join(" "),
                            state_label(&s.state),
                            s.result_count,
                        )
                    })
                })
            })
            .unwrap_or_else(|| "Recent searches…".to_string())
    };

    // Raw (unfiltered) result count of the selected search; drives whether the
    // filter bar is shown. Kept separate from `view_results` so the filter bar
    // only re-renders when results appear/disappear, never on every keystroke.
    let raw_count = move || {
        search
            .selected
            .get()
            .and_then(|id| search.results.with(|m| m.get(&id).map(|v| v.len())))
            .unwrap_or(0)
    };

    // The selected search's results after applying the source/text filter and
    // the sort order. Recomputed reactively wherever it's read.
    let view_results = move || {
        let mut v = search
            .selected
            .get()
            .and_then(|id| search.results.with(|m| m.get(&id).cloned()))
            .unwrap_or_default();
        let sf = filter_source.get();
        let q = filter_text.get().to_lowercase();
        v.retain(|r| sf.matches(&r.source) && (q.is_empty() || r.name.to_lowercase().contains(&q)));
        sort_by.get().apply(&mut v);
        v
    };

    // Load the recent-search list once when the tab mounts; the WS keeps it live.
    Effect::new(move |_| {
        refresh_list(search);
    });

    // Reconcile the selected search against its authoritative result count.
    // The summary count (in `search.list`) is kept current by the WS — both the
    // per-result `SearchResult` events and the final `SearchStateChanged`. If it
    // ever exceeds the results we actually hold locally — e.g. live results were
    // missed, or arrived before the search was selected — fetch the full set so
    // the view refreshes on its own instead of needing a manual re-select.
    Effect::new(move |_| {
        let Some(id) = search.selected.get() else {
            return;
        };
        let summary_count = search
            .list
            .with(|l| l.iter().find(|s| s.id == id).map(|s| s.result_count))
            .unwrap_or(0);
        let local_count = search
            .results
            .with(|m| m.get(&id).map(|v| v.len()).unwrap_or(0));
        if summary_count > local_count {
            load_detail(search, id);
        }
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
                    <div class="history-dd">
                        <button
                            class="history-dd-btn"
                            on:click=move |_| dd_open.update(|o| *o = !*o)
                        >
                            <span class="history-dd-label">{current_label}</span>
                            <Icon paths=icons::CHEVRON_DOWN/>
                        </button>
                        <Show when=move || dd_open.get() fallback=|| ()>
                            // Full-screen catcher: any click outside the menu closes it.
                            <div class="history-dd-backdrop" on:click=move |_| dd_open.set(false)></div>
                            <ul class="history-dd-menu">
                                {move || search.list.get().into_iter().map(|s| {
                                    let id = s.id;
                                    let label = format!(
                                        "{} — {} ({})",
                                        s.keywords.join(" "),
                                        state_label(&s.state),
                                        s.result_count,
                                    );
                                    view! {
                                        <li
                                            class=move || if search.selected.get() == Some(id) {
                                                "history-dd-item history-dd-item-sel"
                                            } else {
                                                "history-dd-item"
                                            }
                                            on:click=move |_| {
                                                search.selected.set(Some(id));
                                                load_detail(search, id);
                                                dd_open.set(false);
                                            }
                                        >{label}</li>
                                    }
                                }).collect_view()}
                            </ul>
                        </Show>
                    </div>
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

            // ── Filter & sort bar (only when the selection has results) ───
            <Show when=move || { raw_count() > 0 } fallback=|| ()>
                <div class="search-filter-bar">
                    <select
                        class="dl-filter-select"
                        on:change=move |e| {
                            filter_source.set(match event_target_value(&e).as_str() {
                                "rucio" => SourceFilter::Rucio,
                                "emule" => SourceFilter::Emule,
                                _ => SourceFilter::All,
                            });
                        }
                    >
                        <option value="all">"All sources"</option>
                        <option value="rucio">"Rucio"</option>
                        <option value="emule">"eMule"</option>
                    </select>
                    <select
                        class="dl-filter-select"
                        prop:value="size-desc"
                        on:change=move |e| {
                            sort_by.set(match event_target_value(&e).as_str() {
                                "name-asc" => SortBy::NameAsc,
                                "name-desc" => SortBy::NameDesc,
                                "size-asc" => SortBy::SizeAsc,
                                _ => SortBy::SizeDesc,
                            });
                        }
                    >
                        <option value="size-desc">"Largest first"</option>
                        <option value="size-asc">"Smallest first"</option>
                        <option value="name-asc">"Name (A→Z)"</option>
                        <option value="name-desc">"Name (Z→A)"</option>
                    </select>
                    <input
                        type="text"
                        class="dl-filter-input"
                        placeholder="Filter results…"
                        prop:value=move || filter_text.get()
                        on:input=move |e| filter_text.set(event_target_value(&e))
                    />
                </div>
            </Show>

            <div class="tab-scroll">
            {move || {
                let sel = search.selected.get();
                if sel.is_none() {
                    return view! {
                        <div class="empty-state"><p>"Search for files, or pick a recent search"</p></div>
                    }.into_any();
                }
                let raw = raw_count();
                let summary = sel.and_then(|id| {
                    search.list.with(|l| l.iter().find(|s| s.id == id).cloned())
                });
                let running = summary.as_ref().map(|s| s.state.clone()) == Some(SearchState::Running);
                // Whether the eMule/Kad leg is queued behind another search.
                let emule_queued = summary.as_ref().is_some_and(|s| s.emule_queued);
                if raw == 0 {
                    return if running {
                        view! {
                            <div class="empty-state empty-searching">
                                <span class="spinner spinner-lg"></span>
                                <p class="searching-indicator">
                                    {if emule_queued { "Waiting for an eMule search turn…" } else { "Searching…" }}
                                </p>
                            </div>
                        }.into_any()
                    } else {
                        view! { <div class="empty-state"><p>"No results"</p></div> }.into_any()
                    };
                }
                let results = view_results();
                let shown = results.len();
                let count_text = if shown == raw {
                    format!("{raw} result(s)")
                } else {
                    format!("{shown} of {raw} result(s)")
                };
                view! {
                    <div class="results-header">
                        <span class="results-count">{count_text}</span>
                        {running.then(|| view! {
                            <span class="results-searching">
                                <span class="spinner"></span>
                                {if emule_queued { "eMule queued…" } else { "searching…" }}
                            </span>
                        })}
                    </div>
                    {if results.is_empty() {
                        view! { <div class="empty-state"><p>"No results match the filter"</p></div> }.into_any()
                    } else {
                        view! {
                            <ul class="results-list">
                                <For
                                    each=move || view_results()
                                    key=|r| r.result_id
                                    children=move |r| view! { <ResultRow result=r downloads=downloads/> }
                                />
                            </ul>
                        }.into_any()
                    }}
                }.into_any()
            }}
            </div>
        </div>
    }
}

/// Transient per-row state covering only the request window (before the new
/// download row shows up in the downloads list) plus the error case.
#[derive(Clone, Copy, PartialEq)]
enum LocalState {
    Idle,
    Sending,
    Error,
}

/// Extract the content hash from a result's download link so it can be matched
/// against `DownloadResponse::root_hash`. Rucio magnets are `rucio:<hash>?…`;
/// ed2k links are `ed2k://|file|<name>|<size>|<hash>|/`.
fn link_hash(link: &str) -> Option<String> {
    if let Some(rest) = link.strip_prefix("rucio:") {
        let h = rest.split('?').next().unwrap_or("");
        if !h.is_empty() {
            return Some(h.to_ascii_lowercase());
        }
    } else if link.starts_with("ed2k://") {
        let parts: Vec<&str> = link.split('|').collect();
        if let Some(h) = parts.get(4)
            && h.len() == 32
        {
            return Some(h.to_ascii_lowercase());
        }
    }
    None
}

/// The state of the download matching this link, if any is in the list.
fn matching_state(link: &str, downloads: &[DownloadResponse]) -> Option<DownloadState> {
    let h = link_hash(link)?;
    downloads
        .iter()
        .find(|d| d.root_hash.eq_ignore_ascii_case(&h))
        .map(|d| d.state.clone())
}

#[component]
fn ResultRow(result: SearchResult, downloads: RwSignal<Vec<DownloadResponse>>) -> impl IntoView {
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

    let local = RwSignal::new(LocalState::Idle);

    // Live download state for this result, matched by hash against the
    // downloads list. This is the source of truth that survives tab switches,
    // unlike the transient `local` state.
    let link_for_state = link.clone();
    let dl_state = move || {
        link_for_state
            .as_deref()
            .and_then(|l| downloads.with(|v| matching_state(l, v)))
    };

    // Fire the download request, then refresh so the new/updated row appears in
    // the downloads list and `dl_state` takes over the UI. A Callback is used
    // so the reactive action block can invoke it without moving it.
    let trigger = Callback::new(move |()| {
        let (Some(l), p) = (link.clone(), provider.clone()) else {
            return;
        };
        local.set(LocalState::Sending);
        spawn_local(async move {
            let outcome = api_start_download(l, p).await;
            refresh_downloads(downloads).await;
            local.set(if outcome == DlOutcome::Error {
                LocalState::Error
            } else {
                LocalState::Idle
            });
        });
    });

    view! {
        <li class="result-row">
            <span class="result-name">{result.name}</span>
            <span class="result-size">{format_size(result.size)}</span>
            <span class=source_css>{source_label}</span>
            <span class="result-action">
            {move || {
                if !can_download {
                    return view! { <span class="result-no-link">"—"</span> }.into_any();
                }
                let derived = dl_state();
                // Absent, or in a re-downloadable terminal state → offer the button.
                let show_button = matches!(
                    derived,
                    None | Some(DownloadState::Failed) | Some(DownloadState::Cancelled)
                );
                if show_button {
                    return match local.get() {
                        LocalState::Sending => view! {
                            <span class="result-dl-status">"Adding…"</span>
                        }.into_any(),
                        other => {
                            let retry = other == LocalState::Error;
                            view! {
                                <button
                                    class="btn-sm btn-primary"
                                    on:click=move |_| trigger.run(())
                                >
                                    {if retry { "Retry" } else { "Download" }}
                                </button>
                            }.into_any()
                        }
                    };
                }
                match derived {
                    Some(DownloadState::Completed) => view! {
                        <span class="result-dl-status result-dl-ok">"Downloaded"</span>
                    }.into_any(),
                    Some(DownloadState::Paused) => view! {
                        <span class="result-dl-status result-dl-have">"Paused"</span>
                    }.into_any(),
                    _ => view! {
                        <span class="result-dl-status result-dl-have">"In downloads"</span>
                    }.into_any(),
                }
            }}
            </span>
        </li>
    }
}
