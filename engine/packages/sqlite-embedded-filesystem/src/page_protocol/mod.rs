//! Filesystem-backed actor SQLite page protocol, shared by Envoy and direct transports.
//! The caller serializes requests for one database and owns its filesystem lease.
pub mod database;
mod prefetch;
mod stage;
use anyhow::{Result, ensure};
use rivet_envoy_protocol as p;
pub use stage::Stage;

/// Runs one request against the database. The request reads the file's head once, here, and
/// every check, read and commit in it uses that head; a commit then carries its own new head,
/// so nothing in a request reads the inode again. The caller serializes requests and owns the
/// volume, so the head cannot move under a request; the next request reads it afresh.
pub async fn execute(
	stage: &mut Option<Stage>,
	pages: &mut database::Database,
	message: p::ToRivet,
) -> Result<p::ToEnvoy> {
	let keeps_stage = keeps_refused_stage(&message);
	let result = async {
		pages.refresh().await?;
		execute_inner(stage, pages, message).await
	}
	.await;
	// Pre-admission refusals retain a resumable stage. Unknown outcomes must abort it.
	let refused = result.as_ref().is_err_and(|error| refusal_hint(keeps_stage, error).is_some());
	if result.is_err() && !refused {
		if let Err(abort_error) = stage::abort(stage).await {
			tracing::error!(request_error = ?result.as_ref().err(), ?abort_error, "staged SQLite abort failed");
			pages
				.fs
				.fence_with(depot_client::fences::FenceReason::StagedWriteAbortFailed);
		}
	}
	result
}

/// Aborts the staged commit a refused request left open, as when its sender stops resending
/// it, so the next staged commit starts afresh instead of meeting the stale stage. A failed
/// abort fences the filesystem, as after any other failed request.
pub async fn abandon_stage(stage: &mut Option<Stage>, pages: &mut database::Database) {
	if let Err(abort_error) = stage::abort(stage).await {
		tracing::error!(?abort_error, "abandoned SQLite stage abort failed");
		pages
			.fs
			.fence_with(depot_client::fences::FenceReason::StagedWriteAbortFailed);
	}
}

/// Whether `error` is a refused cold read. Every cold read of a request, a commit's included, happens before its write is
/// admitted, and a refusal after admission is reported as an undecided write instead, so the
/// identical request may be sent again, against the same stage.
pub fn refused_without_running(error: &anyhow::Error) -> bool {
	matches!(
		error.downcast_ref::<depot_client::filesystem::Error>(),
		Some(depot_client::filesystem::Error::ColdReadRefused)
	)
}

/// Retry hint for an identical page request whose staged transaction remains available.
/// Errors with an admitted write token never qualify: their outcome may still be unknown.
pub fn retry_after_ms(message: &p::ToRivet, error: &anyhow::Error) -> Option<u64> {
	refusal_hint(keeps_refused_stage(message), error)
}

fn keeps_refused_stage(message: &p::ToRivet) -> bool {
	matches!(message,
		p::ToRivet::ToRivetSqliteGetPagesRequest(_)
		| p::ToRivet::ToRivetSqliteCommitRequest(_)
		| p::ToRivet::ToRivetSqliteCommitStageSegmentRequest(_)
		| p::ToRivet::ToRivetSqliteCommitFinalizeRequest(_))
}

fn refusal_hint(keeps_stage: bool, error: &anyhow::Error) -> Option<u64> {
	let error = error.downcast_ref::<depot_client::filesystem::Error>()?;
	if keeps_stage || *error == depot_client::filesystem::Error::ColdReadRefused {
		error.retry_after_ms()
	} else {
		None
	}
}

async fn execute_inner(
	stage: &mut Option<stage::Stage>,
	pages: &mut database::Database,
	message: p::ToRivet,
) -> Result<p::ToEnvoy> {
	match message {
		p::ToRivet::ToRivetSqliteGetPagesRequest(req) => {
			ensure!(req.data.pgnos.len() <= 4096, "too many SQLite pages");
			check_head(req.data.expected_head_txid, pages.root.txid)?;
			ensure!(
				req.data.pgnos.iter().all(|pgno| *pgno > 0),
				"invalid SQLite page"
			);
			let opening = req.data.expected_head_txid.is_none();
			let fetched = pages
				.planned_pages(u64::from(req.request_id), &req.data.pgnos, opening)
				.await?;
			Ok(p::ToEnvoy::ToEnvoySqliteGetPagesResponse(
				p::ToEnvoySqliteGetPagesResponse {
					request_id: req.request_id,
					data: p::SqliteGetPagesResponse::SqliteGetPagesOk(p::SqliteGetPagesOk {
						pages: fetched,
						head_txid: Some(pages.root.txid),
					}),
				},
			))
		}
		p::ToRivet::ToRivetSqliteCommitRequest(req) => {
			check_write_head(req.data.expected_head_txid, pages)?;
			stage::abort(stage).await?;
			let txid = pages
				.commit(req.data.dirty_pages, req.data.db_size_pages)
				.await?;
			Ok(p::ToEnvoy::ToEnvoySqliteCommitResponse(
				p::ToEnvoySqliteCommitResponse {
					request_id: req.request_id,
					data: p::SqliteCommitResponse::SqliteCommitOk(p::SqliteCommitOk {
						head_txid: Some(txid),
					}),
				},
			))
		}

		message => stage::execute(stage, pages, message).await,
	}
}

fn check_write_head(expected: Option<u64>, pages: &database::Database) -> Result<()> {
	// Clients preload the head before writing. A legacy absent head is only
	// valid for an empty database, never an unconditional overwrite.
	if expected.is_none() {
		ensure!(pages.root.size_pages == 0, HeadFenceMismatch);
	}
	check_head(expected, pages.root.txid)
}

fn check_head(expected: Option<u64>, actual: u64) -> Result<()> {
	ensure!(
		expected.is_none_or(|expected| expected == actual),
		HeadFenceMismatch
	);
	Ok(())
}

#[derive(Debug)]
pub struct HeadFenceMismatch;
impl std::fmt::Display for HeadFenceMismatch {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("SQLite head fence mismatch")
	}
}
impl std::error::Error for HeadFenceMismatch {}
