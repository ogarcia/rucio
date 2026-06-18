//! `rucio category list`, `rucio category add <name> [--dir]`,
//! `rucio category set <id> <name> [--dir]`, `rucio category remove <id>`

use anyhow::Result;
use rust_i18n::t;
use tabled::builder::Builder;

use crate::client::ApiClient;
use crate::color;

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_categories().await?;
    if resp.categories.is_empty() {
        println!("{}", t!("category.none"));
        return Ok(());
    }

    let mut table = Builder::new();
    table.push_record([
        t!("category.col.id").to_string(),
        t!("category.col.name").to_string(),
        t!("category.col.dir").to_string(),
        t!("category.col.color").to_string(),
        t!("category.col.match").to_string(),
    ]);
    for c in &resp.categories {
        table.push_record([
            c.id.to_string(),
            c.name.clone(),
            // No pinned dir → downloads go to the global download directory.
            match &c.download_dir {
                Some(d) => color::value(d),
                None => t!("category.global").to_string(),
            },
            c.color.clone().unwrap_or_else(|| "-".to_string()),
            c.match_keywords.clone().unwrap_or_else(|| "-".to_string()),
        ]);
    }

    println!("{}", table.build());
    Ok(())
}

pub async fn add(
    client: &ApiClient,
    name: &str,
    dir: Option<&str>,
    color: Option<&str>,
    keywords: Option<&str>,
) -> Result<()> {
    match client.create_category(name, dir, color, keywords).await {
        Ok(c) => println!(
            "{}",
            color::success(&t!("category.created", name = c.name, id = c.id))
        ),
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn set(
    client: &ApiClient,
    id: i64,
    name: &str,
    dir: Option<&str>,
    color: Option<&str>,
    keywords: Option<&str>,
) -> Result<()> {
    match client.update_category(id, name, dir, color, keywords).await {
        Ok(()) => println!("{}", color::success(&t!("category.updated", id = id))),
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, id: i64) -> Result<()> {
    match client.delete_category(id).await {
        Ok(()) => println!("{}", color::success(&t!("category.removed", id = id))),
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}
