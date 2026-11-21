use depot_client::filesystem::{self as fs, transport::*};
use depot_types::{VolumeLogKey, payload_codec::payload_digest};
use depot_volume::volume_client_machine::{
    runner::{CommitTicket, LogHistory},
    types::{WriteError, WriteOutcome, WriteToken},
};
use futures_util::future::BoxFuture;
use rivet_data::generated::{depot_log_shard_v1::*, shared_depot_types_v1::*};
use rivet_envoy_protocol as p;
use sqlite_embedded_filesystem::page_protocol::{self, database::Database};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};

#[derive(Default)]
struct Log {
    entries: Mutex<Vec<VolumeLogEntry>>,
    refuse: AtomicBool,
    fenced: AtomicBool,
    attempts: AtomicUsize,
    held_committed: AtomicU64,
    settle_on_barrier: AtomicBool,
}

fn position(sequence: u64) -> VolumeLogPosition {
    VolumeLogPosition { volume_seq: sequence, client_epoch: 1 }
}

impl Journal for Log {
    fn spawn_cleanup(&self, cleanup: BoxFuture<'static, ()>) -> fs::Result<()> {
        tokio::spawn(cleanup);
        Ok(())
    }
    fn status(&self) -> fs::Result<LogStatus> {
        if self.fenced.load(Ordering::SeqCst) { return Err(fs::Error::Fenced); }
        let sequence = self.entries.lock().unwrap().len() as u64 + 1;
        Ok(LogStatus {
            key: VolumeLogKey(rivet_data::generated::shared_depot_types_v1::VolumeLogKey {
                volume_key: VolumeKey { partition_key: b"test".to_vec().into(), name: None },
                volume_incarnation: [1; 16],
            }),
            cold: position(0), committed: match self.held_committed.load(Ordering::SeqCst) {
                0 => sequence, held => held,
            }, next_seq: sequence + 1,
            client_epoch: 1, max_payload_bytes: 8 * 1024 * 1024, retained_bytes: 0,
        })
    }
    fn entry_version(&self) -> fs::Result<u16> { Ok(rivet_data::DEPOT_FS_ENTRY_VERSION) }
    fn reads_admitted(&self) -> bool { true }
    fn recover(&self) -> BoxFuture<'_, fs::Result<LogHistory>> {
        Box::pin(async move {
            let entries = self.entries.lock().unwrap().clone();
            Ok(LogHistory { cold: position(0), committed: entries.len() as u64 + 1,
                empty: entries.is_empty(), complete: true, entries })
        })
    }
    fn admit(&self, payload: vbare::Bytes) -> BoxFuture<'_, fs::Result<WriteOutcome>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.refuse.load(Ordering::SeqCst) {
                return Err(fs::Error::Log(WriteError::Overloaded { retry_after_ms: 275, token: None }));
            }
            let mut entries = self.entries.lock().unwrap();
            let sequence = entries.len() as u64 + 2;
            let digest = payload_digest(&payload);
            entries.push(VolumeLogEntry {
                written_at_unix_ms: 0, volume_seq: sequence, client_epoch: 1,
                body: VolumeLogEntryBody::PayloadWrite(PayloadWrite {
                    payload_family: rivet_data::families::VersionFamily::DEPOT_FS_ENTRY.0,
                    payload_version: rivet_data::DEPOT_FS_ENTRY_VERSION,
                    client_seq: sequence, request_id: [2; 16], payload_digest: digest,
                    codec: PayloadCodec::None, payload,
                }),
            });
            Ok(WriteOutcome { volume_seq: sequence, position: position(sequence),
                token: WriteToken { payload_digest: digest, volume_incarnation: [1; 16],
                    client_seq: sequence, request_id: [2; 16] },
                applied_earlier: false, committed: Some(sequence) })
        })
    }
    fn barrier(&self) -> fs::Result<CommitTicket> {
        if self.settle_on_barrier.swap(false, Ordering::SeqCst) {
            self.held_committed.store(0, Ordering::SeqCst);
        }
        if self.held_committed.load(Ordering::SeqCst) != 0 {
            return Ok(Box::pin(async { Err(WriteError::Overloaded { retry_after_ms: 275, token: None }) }));
        }
        let committed = self.status()?.committed;
        Ok(Box::pin(async move { Ok(committed) }))
    }
    fn close(&self) -> BoxFuture<'_, fs::Result<()>> { Box::pin(async { Ok(()) }) }
    fn fence(&self) { self.fenced.store(true, Ordering::SeqCst); }
}

struct NoColdReads;
impl ColdStore for NoColdReads {
    fn open_prefetching(&self, _: VolumeLogKey, _: VolumeLogPosition, _: OpenPrefetch, _: u64)
        -> BoxFuture<'_, fs::Result<OpenedSnapshot>> { panic!("fresh volume has no cold rows") }
    fn open_latest_prefetching(&self, _: VolumeLogKey, _: VolumeLogPosition, _: OpenPrefetch, _: u64)
        -> BoxFuture<'_, fs::Result<OpenedSnapshot>> { panic!("fresh volume has no cold rows") }
    fn claim_snapshot(&self, _: VolumeLogKey, _: VolumeLogPosition) -> Arc<dyn ColdSnapshot> {
        panic!("test retains its fresh volume overlay")
    }
    fn observed_applied(&self) -> u64 { 0 }
}

fn begin(base: u64) -> p::ToRivet {
    p::ToRivet::ToRivetSqliteCommitStageBeginRequest(p::ToRivetSqliteCommitStageBeginRequest {
        request_id: 1, data: p::SqliteCommitStageBeginRequest { actor_id: "test".into(), expected_generation: None, expected_head_txid: Some(base) },
    })
}
fn segment(txid: u64) -> p::ToRivet {
    p::ToRivet::ToRivetSqliteCommitStageSegmentRequest(p::ToRivetSqliteCommitStageSegmentRequest {
        request_id: 2, data: p::SqliteCommitStageSegmentRequest { actor_id: "test".into(), expected_generation: None, txid, first_pgno: 0,
            dirty_pages: vec![p::SqliteDirtyPage { pgno: 1, bytes: vec![42; 4096].into() }] },
    })
}
fn finalize(txid: u64) -> p::ToRivet {
    p::ToRivet::ToRivetSqliteCommitFinalizeRequest(p::ToRivetSqliteCommitFinalizeRequest {
        request_id: 3, data: p::SqliteCommitFinalizeRequest { actor_id: "test".into(), expected_generation: None, now_ms: 0, txid, new_db_size_pages: 1,
            segment_first_pgnos: vec![0] },
    })
}

async fn refusal_keeps_stage(refuse_finalize: bool) {
    let log = Arc::new(Log::default());
    let filesystem = fs::Filesystem::open_staging_empty(log.clone(), Arc::new(NoColdReads),
        Arc::new(rivet_env::TokioClock::new().unwrap()), fs::Config::default(), None,
        OpenPrefetch::default()).await.unwrap();
    let mut database = Database::from_filesystem(filesystem).await.unwrap();
    let mut stage = None;
    let base = database.root.txid;
    let txid = base + 1;
    page_protocol::execute(&mut stage, &mut database, begin(base)).await.unwrap();
    if refuse_finalize {
        page_protocol::execute(&mut stage, &mut database, segment(txid)).await.unwrap();
    }
    let message = if refuse_finalize { finalize(txid) } else { segment(txid) };
    let before = log.attempts.load(Ordering::SeqCst);
    log.refuse.store(true, Ordering::SeqCst);
    let error = page_protocol::execute(&mut stage, &mut database, message.clone()).await.unwrap_err();
    assert_eq!(error.downcast_ref::<fs::Error>().unwrap().retry_after_ms(), Some(275));
    assert_eq!(page_protocol::retry_after_ms(&message, &error), Some(275));
    assert!(stage.is_some(), "a refusal must retain the identical staged transaction");
    assert!(!log.fenced.load(Ordering::SeqCst), "a proven refusal must not fence");
    assert_eq!(log.attempts.load(Ordering::SeqCst), before + 1, "a refusal must not attempt abort");
    log.refuse.store(false, Ordering::SeqCst);
    page_protocol::execute(&mut stage, &mut database, message).await.unwrap();
    if !refuse_finalize {
        page_protocol::execute(&mut stage, &mut database, finalize(txid)).await.unwrap();
    }
    assert!(stage.is_none());
    assert!(database.root.txid > base);
    assert_eq!(database.root.txid, log.status().unwrap().committed);
    assert_eq!(database.read_pages(&[1]).await.unwrap()[0].as_deref(), Some(&[42; 4096][..]));
}

#[tokio::test]
async fn overloaded_segment_retains_stage_without_abort() { refusal_keeps_stage(false).await; }

#[tokio::test]
async fn overloaded_finalize_retains_stage_without_abort() { refusal_keeps_stage(true).await; }

#[tokio::test]
async fn stage_begin_rechecks_head_after_settling_an_undecided_commit() {
    let log = Arc::new(Log::default());
    let filesystem = fs::Filesystem::open_staging_empty(log.clone(), Arc::new(NoColdReads),
        Arc::new(rivet_env::TokioClock::new().unwrap()), fs::Config::default(), None,
        OpenPrefetch::default()).await.unwrap();
    let mut database = Database::from_filesystem(filesystem).await.unwrap();
    let mut stage = None;
    let base = database.root.txid;
    page_protocol::execute(&mut stage, &mut database, begin(base)).await.unwrap();
    page_protocol::execute(&mut stage, &mut database, segment(base + 1)).await.unwrap();
    log.held_committed.store(log.status().unwrap().committed, Ordering::SeqCst);
    let error = page_protocol::execute(&mut stage, &mut database, finalize(base + 1)).await.unwrap_err();
    assert!(matches!(error.downcast_ref::<fs::Error>(),
        Some(fs::Error::Log(WriteError::QuorumUnavailable { token: Some(_) }))));
    assert!(stage.is_none());
    database.refresh().await.unwrap();
    let stale_head = database.root.txid;
    log.settle_on_barrier.store(true, Ordering::SeqCst);
    log.refuse.store(true, Ordering::SeqCst);
    let attempts = log.attempts.load(Ordering::SeqCst);
    let error = page_protocol::execute(&mut stage, &mut database, begin(stale_head)).await
        .expect_err("stage begin must reject the head that its own barrier made stale");
    assert!(error.is::<page_protocol::HeadFenceMismatch>(), "{error:#}");
    assert!(stage.is_none());
    assert_eq!(log.attempts.load(Ordering::SeqCst), attempts,
        "an empty stage has no journal entries to abort");
    assert!(!database.fs.is_fenced());
    assert!(!log.fenced.load(Ordering::SeqCst));
    log.refuse.store(false, Ordering::SeqCst);
    database.refresh().await.unwrap();
    let committed_head = database.root.txid;
    assert!(committed_head > stale_head);
    page_protocol::execute(&mut stage, &mut database, begin(committed_head)).await.unwrap();
    page_protocol::execute(&mut stage, &mut database, segment(committed_head + 1)).await.unwrap();
    page_protocol::execute(&mut stage, &mut database, finalize(committed_head + 1)).await.unwrap();
    assert!(stage.is_none());
    assert_eq!(database.read_pages(&[1]).await.unwrap()[0].as_deref(), Some(&[42; 4096][..]));
}

#[tokio::test]
async fn abort_with_an_admitted_segment_still_requires_a_journal_entry() {
    let log = Arc::new(Log::default());
    let filesystem = fs::Filesystem::open_staging_empty(log.clone(), Arc::new(NoColdReads),
        Arc::new(rivet_env::TokioClock::new().unwrap()), fs::Config::default(), None,
        OpenPrefetch::default()).await.unwrap();
    let mut database = Database::from_filesystem(filesystem).await.unwrap();
    let mut stage = None;
    let base = database.root.txid;
    page_protocol::execute(&mut stage, &mut database, begin(base)).await.unwrap();
    page_protocol::execute(&mut stage, &mut database, segment(base + 1)).await.unwrap();
    let attempts = log.attempts.load(Ordering::SeqCst);
    log.refuse.store(true, Ordering::SeqCst);
    page_protocol::abandon_stage(&mut stage, &mut database).await;
    assert!(stage.is_none());
    assert_eq!(log.attempts.load(Ordering::SeqCst), attempts + 1);
    assert!(database.fs.is_fenced(), "admitted data must not be discarded locally");
}

#[test]
fn admitted_overload_never_allows_a_new_staged_request() {
    let token = WriteToken { payload_digest: [3; 16], volume_incarnation: [1; 16],
        client_seq: 2, request_id: [2; 16] };
    let error = anyhow::Error::new(fs::Error::Log(WriteError::Overloaded {
        retry_after_ms: 275, token: Some(token),
    }));
    for message in [segment(1), finalize(1)] {
        assert_eq!(page_protocol::retry_after_ms(&message, &error), None);
        for error in [fs::Error::Fenced, fs::Error::Conflict, fs::Error::DeadlineExceeded] {
            assert_eq!(page_protocol::retry_after_ms(&message, &error.into()), None);
        }
    }
}
