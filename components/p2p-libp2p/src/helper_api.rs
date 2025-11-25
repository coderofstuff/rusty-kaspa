//! JSON helper/control API definitions will live here. Placeholder for now.

use serde::Deserialize;

/// Placeholder helper control surface.
#[derive(Debug, Default, Clone)]
pub struct HelperApi;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HelperError {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("unknown action")]
    UnknownAction,
}

#[derive(Debug, Deserialize)]
struct HelperRequest {
    action: String,
}

impl HelperApi {
    pub fn handle_json(&self, json: &str) -> Result<String, HelperError> {
        let req: HelperRequest = serde_json::from_str(json).map_err(|e| HelperError::Invalid(e.to_string()))?;
        match req.action.as_str() {
            "status" => Ok(r#"{"ok":true}"#.to_string()),
            _ => Err(HelperError::UnknownAction),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_helper_api_exists() {
        let api = HelperApi::default();
        assert_eq!(format!("{api:?}"), "HelperApi");
    }

    #[test]
    fn helper_status_ok() {
        let api = HelperApi::default();
        let resp = api.handle_json(r#"{"action":"status"}"#).unwrap();
        assert!(resp.contains(r#""ok":true"#));
    }

    #[test]
    fn helper_unknown_action_errors() {
        let api = HelperApi::default();
        let resp = api.handle_json(r#"{"action":"refresh"}"#);
        assert!(matches!(resp, Err(HelperError::UnknownAction)));
    }

    #[test]
    fn helper_malformed_errors() {
        let api = HelperApi::default();
        let resp = api.handle_json(r#"{"action":1"#);
        assert!(matches!(resp, Err(HelperError::Invalid(_))));
    }
}
