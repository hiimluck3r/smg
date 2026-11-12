//! The parse endpoints: a model's output text read whole through the Symphony table the request
//! names (`routers::grpc::regular::symphony`), for its tool calls (`/parse/function_call`) or its
//! reasoning (`/parse/reasoning`). A table answers to its own name and to the older parser names
//! the gateway maps onto it (`routers::grpc::utils::ALIASES`; `passthrough` is `plain`); any other
//! name is a 400 that lists the tables. The text is read as one delta after an empty prompt, so it
//! reads as a model's output would from its first token.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use openai_protocol::{
    chat::ChatCompletionMessage,
    common::Tool,
    parser::{ParseFunctionCallRequest, SeparateReasoningRequest},
};
use symphony::{adapt, formats::Table, Declared, Engine, EngineFinish, Events, Input, Parser};
use tracing::error;

use crate::routers::grpc::utils::answering_to;

/// Helper to create error responses
fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": message,
            "success": false
        })),
    )
        .into_response()
}

/// `text` read whole with the table `name` selects, over the declared `tools`, as the message its
/// events fold into: content, reasoning and calls. Without `calls` the table's variant that reads
/// no calls reads, so a call's text stays content (the reasoning endpoint separates the thought
/// alone, as before). A name no table answers to is the 400 naming the tables; a table that
/// refuses the three lifecycle steps, which are in order, is a 500.
fn read_whole(
    kind: &str,
    name: &str,
    text: &str,
    tools: Vec<Tool>,
    calls: bool,
) -> Result<ChatCompletionMessage, Response> {
    let Some(table) = answering_to(name) else {
        let tables: Vec<&str> = Table::ALL.iter().map(|table| table.name()).collect();
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            &format!(
                "Unknown {kind} parser: {name}; the tables are {}",
                tables.join(", ")
            ),
        ));
    };
    // The call parse reads no thought: the text beside the calls, a thought's markers
    // included, comes back as written, as the older parser left it.
    let mut engine = if calls {
        Engine::new(
            (*table.shared()).clone().without_reasoning(),
            Declared::shared(tools.into()),
        )
    } else {
        Engine::new(
            (*table.shared()).clone().without_calls(),
            Declared::default(),
        )
    };
    let mut events = Events::new();
    for step in [
        Input::Prompt {
            token_ids: &[],
            text: "",
        },
        Input::Delta {
            token_ids: &[],
            text,
            spans: &[],
        },
        Input::End {
            finish: EngineFinish::Stop,
        },
    ] {
        engine.feed(step, &mut events).map_err(|e| {
            error!(table = table.name(), error = %e, "the table refused the text");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!(
                    "Failed to read the text with the {} table: {e}",
                    table.name()
                ),
            )
        })?;
    }
    Ok(adapt::chat::message(0, events.as_slice()).message)
}

/// The tool calls in `text` as the table named by `tool_call_parser` reads them, and the content
/// left beside them, a thought's markers included: the call parse reads no thought.
pub fn parse_function_call(req: &ParseFunctionCallRequest) -> Response {
    match read_whole(
        "tool",
        &req.tool_call_parser,
        &req.text,
        req.tools.clone(),
        true,
    ) {
        Ok(message) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "remaining_text": message.content.unwrap_or_default(),
                "tool_calls": message.tool_calls.unwrap_or_default(),
                "success": true
            })),
        )
            .into_response(),
        Err(response) => response,
    }
}

/// The reasoning in `text` as the table named by `reasoning_parser` reads it, and the text left
/// beside it, a call's text included.
pub fn parse_reasoning(req: &SeparateReasoningRequest) -> Response {
    match read_whole(
        "reasoning",
        &req.reasoning_parser,
        &req.text,
        Vec::new(),
        false,
    ) {
        Ok(message) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "normal_text": message.content.unwrap_or_default(),
                "reasoning_text": message.reasoning_content.unwrap_or_default(),
                "success": true
            })),
        )
            .into_response(),
        Err(response) => response,
    }
}
