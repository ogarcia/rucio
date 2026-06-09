//! Download-categories endpoints: `/api/v1/categories`.
//!
//! A category groups downloads and may pin its own download directory. Creating,
//! updating or deleting one reconciles the protected/shared directory set (see
//! [`crate::reconcile_protected_dirs`]) so a category directory is created on
//! disk and protected from removal, and a directory that is no longer used is
//! demoted.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use rucio_core::api::categories::{CategoriesResponse, CategoryRequest, CategoryResponse};

use crate::api::AppState;
use crate::db;

fn to_response(c: db::categories::Category) -> CategoryResponse {
    CategoryResponse {
        id: c.id,
        name: c.name,
        download_dir: c.download_dir,
        color: c.color,
        match_keywords: c.match_keywords,
    }
}

/// Validate an optional badge colour: a non-blank value must be a hex string
/// `#rgb` or `#rrggbb` (what a colour picker sends). Blank/None is fine.
fn validate_color(color: Option<&str>) -> Result<(), StatusCode> {
    if let Some(c) = color.map(str::trim).filter(|c| !c.is_empty()) {
        let ok = c.starts_with('#')
            && matches!(c.len(), 4 | 7)
            && c[1..].chars().all(|ch| ch.is_ascii_hexdigit());
        if !ok {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    Ok(())
}

/// Validate a category's directory: a non-blank value must be an absolute path
/// that we can create. Blank/None is fine — the category then uses the global
/// download dir. Returns `BAD_REQUEST` on a relative path or a dir we can't make.
fn validate_dir(dir: Option<&str>) -> Result<(), StatusCode> {
    if let Some(d) = dir.map(str::trim).filter(|d| !d.is_empty()) {
        let p = std::path::Path::new(d);
        if !p.is_absolute() {
            return Err(StatusCode::BAD_REQUEST);
        }
        if let Err(e) = std::fs::create_dir_all(p) {
            tracing::warn!(dir = d, "Category directory is not usable: {e}");
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    Ok(())
}

/// Map a category insert/update DB error to a status code: a UNIQUE-name clash
/// is a 409, anything else a 500.
fn write_err<E: std::fmt::Display>(e: E) -> StatusCode {
    let msg = e.to_string();
    if msg.contains("UNIQUE") {
        StatusCode::CONFLICT
    } else {
        tracing::error!("categories write: {msg}");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// List categories.
///
/// Returns every download category, ordered by name.
#[utoipa::path(
    get,
    path = "/api/v1/categories",
    tag = "categories",
    responses((status = 200, description = "All categories", body = CategoriesResponse)),
)]
pub async fn list_categories(
    State(state): State<AppState>,
) -> Result<Json<CategoriesResponse>, StatusCode> {
    let categories = db::categories::list(&state.db)
        .await
        .map_err(|e| {
            tracing::error!("list categories: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .into_iter()
        .map(to_response)
        .collect();
    Ok(Json(CategoriesResponse { categories }))
}

/// Create a category.
///
/// `name` must be unique. `download_dir`, if given, must be an absolute path
/// that the daemon can create; it is created and protected from removal. Omit
/// it (or send null) to file the category's downloads under the global dir.
#[utoipa::path(
    post,
    path = "/api/v1/categories",
    tag = "categories",
    request_body = CategoryRequest,
    responses(
        (status = 200, description = "Category created", body = CategoryResponse),
        (status = 400, description = "Empty name or an unusable download_dir"),
        (status = 409, description = "A category with that name already exists"),
    )
)]
pub async fn create_category(
    State(state): State<AppState>,
    Json(req): Json<CategoryRequest>,
) -> Result<Json<CategoryResponse>, StatusCode> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    validate_dir(req.download_dir.as_deref())?;
    validate_color(req.color.as_deref())?;

    let id = db::categories::create(
        &state.db,
        name,
        req.download_dir.as_deref(),
        req.color.as_deref(),
        req.match_keywords.as_deref(),
        crate::now_secs(),
    )
    .await
    .map_err(write_err)?;

    reconcile(&state).await;

    let cat = db::categories::get(&state.db, id)
        .await
        .ok()
        .flatten()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(to_response(cat)))
}

/// Update a category.
///
/// Replaces the name and directory. Same validation as create. Returns 404 if
/// there is no category with that id.
#[utoipa::path(
    put,
    path = "/api/v1/categories/{id}",
    tag = "categories",
    params(("id" = i64, Path, description = "Category id")),
    request_body = CategoryRequest,
    responses(
        (status = 200, description = "Category updated", body = CategoryResponse),
        (status = 400, description = "Empty name or an unusable download_dir"),
        (status = 404, description = "No such category"),
        (status = 409, description = "A category with that name already exists"),
    )
)]
pub async fn update_category(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<CategoryRequest>,
) -> Result<Json<CategoryResponse>, StatusCode> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if db::categories::get(&state.db, id)
        .await
        .ok()
        .flatten()
        .is_none()
    {
        return Err(StatusCode::NOT_FOUND);
    }
    validate_dir(req.download_dir.as_deref())?;
    validate_color(req.color.as_deref())?;

    db::categories::update(
        &state.db,
        id,
        name,
        req.download_dir.as_deref(),
        req.color.as_deref(),
        req.match_keywords.as_deref(),
    )
    .await
    .map_err(write_err)?;

    reconcile(&state).await;

    let cat = db::categories::get(&state.db, id)
        .await
        .ok()
        .flatten()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(to_response(cat)))
}

/// Delete a category.
///
/// Downloads filed under it fall back to the global download dir (their
/// `category_id` is cleared). Its directory is demoted to an ordinary,
/// removable share.
#[utoipa::path(
    delete,
    path = "/api/v1/categories/{id}",
    tag = "categories",
    params(("id" = i64, Path, description = "Category id")),
    responses(
        (status = 204, description = "Category deleted"),
        (status = 404, description = "No such category"),
    )
)]
pub async fn delete_category(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    match db::categories::delete(&state.db, id).await {
        Ok(true) => {
            reconcile(&state).await;
            StatusCode::NO_CONTENT
        }
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("delete category: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Reconcile the protected/shared directory set after a category change.
/// Best-effort: a failure is logged but does not fail the request (the row is
/// already persisted; the set is re-reconciled on next startup anyway).
async fn reconcile(state: &AppState) {
    if let Err(e) =
        crate::reconcile_protected_dirs(&state.db, &state.config.storage.download_dir).await
    {
        tracing::warn!("Could not reconcile protected dirs after category change: {e}");
    }
}
