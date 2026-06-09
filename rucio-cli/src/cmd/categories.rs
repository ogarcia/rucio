//! `rucio category list`, `rucio category add <name> [--dir]`,
//! `rucio category set <id> <name> [--dir]`, `rucio category remove <id>`

use anyhow::Result;
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::color;

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_categories().await?;
    if resp.categories.is_empty() {
        println!("No categories.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "ID")]
        id: i64,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Download dir")]
        dir: String,
    }

    let rows: Vec<Row> = resp
        .categories
        .iter()
        .map(|c| Row {
            id: c.id,
            name: c.name.clone(),
            // No pinned dir → downloads go to the global download directory.
            dir: match &c.download_dir {
                Some(d) => color::value(d),
                None => "(global)".to_string(),
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

pub async fn add(client: &ApiClient, name: &str, dir: Option<&str>) -> Result<()> {
    match client.create_category(name, dir).await {
        Ok(c) => println!(
            "{}",
            color::success(&format!("Created category '{}' (id {}).", c.name, c.id))
        ),
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn set(client: &ApiClient, id: i64, name: &str, dir: Option<&str>) -> Result<()> {
    match client.update_category(id, name, dir).await {
        Ok(()) => println!("{}", color::success(&format!("Updated category {id}."))),
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, id: i64) -> Result<()> {
    match client.delete_category(id).await {
        Ok(()) => println!("{}", color::success(&format!("Removed category {id}."))),
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}
