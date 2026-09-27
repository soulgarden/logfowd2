use serde::Deserialize;

use crate::error::EsError;

#[derive(Deserialize)]
struct BulkResponse {
    errors: bool,
    items: Vec<BulkItem>,
}

#[derive(Deserialize)]
struct BulkItem {
    index: BulkItemResult,
}

#[derive(Deserialize)]
struct BulkItemResult {
    status: u16,
    error: Option<serde_json::Value>,
}

pub fn failed_items(body: &str, expected: usize) -> Result<Vec<(usize, String)>, EsError> {
    let response: BulkResponse = serde_json::from_str(body)
        .map_err(|e| EsError::RequestFailed(format!("Invalid bulk response: {e}")))?;
    if response.items.len() != expected {
        return Err(EsError::RequestFailed(format!(
            "Bulk response has {} items for {expected} events",
            response.items.len()
        )));
    }

    let failures: Vec<_> = response
        .items
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let result = item.index;
            if (200..300).contains(&result.status) && result.error.is_none() {
                None
            } else {
                Some((
                    index,
                    format!(
                        "Bulk item status {}: {}",
                        result.status,
                        result.error.unwrap_or(serde_json::Value::Null)
                    ),
                ))
            }
        })
        .collect();

    if response.errors && failures.is_empty() {
        return Err(EsError::RequestFailed(
            "Bulk response reported errors without failed items".to_string(),
        ));
    }
    Ok(failures)
}
