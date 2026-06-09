//! DTOs for the download-categories API (`/api/v1/categories`).
//!
//! A category groups downloads and may pin its own download directory; a
//! download with no category (or a category that pins no directory) lands in
//! the global `storage.download_dir`.

/// Request body for creating or updating a category.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CategoryRequest {
    /// Display name; must be unique.
    pub name: String,
    /// Absolute path where this category's downloads are saved. Omit or leave
    /// null to use the global download directory.
    #[serde(default)]
    pub download_dir: Option<String>,
}

/// A category as returned by the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CategoryResponse {
    pub id: i64,
    pub name: String,
    #[serde(default)]
    pub download_dir: Option<String>,
}

/// GET /api/v1/categories
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CategoriesResponse {
    pub categories: Vec<CategoryResponse>,
}

/// Request body for assigning (or clearing) a download's category.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SetCategoryRequest {
    /// Category id, or null to remove the assignment (back to the global dir).
    #[serde(default)]
    pub category_id: Option<i64>,
}
