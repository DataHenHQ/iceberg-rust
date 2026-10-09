// Copyright DataHen. This file is part of DataHen's fork of Apache iceberg-rust and was not
// contributed to the Apache Software Foundation. See DATAHEN_FORK.md.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

//! Row-level changes committed as one snapshot: new data files, new delete files, and the
//! removal of delete files they replace. Mirrors `org.apache.iceberg.RowDelta`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A file is identified by its path and, for a blob inside a shared file (a deletion vector in
/// a Puffin file), the blob's offset.
type FileKey = (String, Option<i64>);

fn key_of(file: &DataFile) -> FileKey {
    (file.file_path.clone(), file.content_offset)
}

/// Transaction action for row-level modifications.
pub struct RowDeltaAction {
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_files: Vec<DataFile>,
    replace: bool,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            removed_files: vec![],
            replace: false,
        }
    }

    /// A rewrite: the same rows in new files. Data files and the delete files that applied to
    /// them are removed, and their compacted replacements added, in ONE snapshot whose operation
    /// is `replace` — `org.apache.iceberg.RewriteFiles` with delete files. The caller guarantees
    /// that the table's rows are unchanged; nothing here can check it.
    pub(crate) fn new_replace() -> Self {
        Self {
            replace: true,
            ..Self::new()
        }
    }

    /// Add data files (inserted rows, or the new versions of updated rows).
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add delete files: position deletes, deletion vectors or equality deletes.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Remove delete files this commit replaces — in format version 3, the previous deletion
    /// vector of a data file that is given a new one.
    pub fn remove_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_files.extend(delete_files);
        self
    }

    /// Remove data files (a copy-on-write region of the same commit).
    pub fn remove_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.removed_files.clone(),
        )
        .with_added_delete_files(self.added_delete_files.clone());

        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;
        snapshot_producer.validate_duplicate_files().await?;

        if self.replace && self.removed_files.is_empty() {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "A replace must remove at least one file",
            ));
        }
        let props = table.metadata().properties();
        let merge_enabled = props
            .get(MANIFEST_MERGE_ENABLED)
            .map(|v| v == "true")
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);
        let min_count_to_merge = props
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);

        let operation = RowDeltaOperation {
            replace: self.replace,
            merge_delete_manifests_at: merge_enabled.then_some(min_count_to_merge),
            adds_data: !self.added_data_files.is_empty(),
            adds_deletes: !self.added_delete_files.is_empty(),
            removed: self.removed_files.iter().map(key_of).collect(),
        };
        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// `commit.manifest-merge.enabled`, as in Java: whether a commit may merge manifests.
const MANIFEST_MERGE_ENABLED: &str = "commit.manifest-merge.enabled";
const MANIFEST_MERGE_ENABLED_DEFAULT: bool = true;
/// `commit.manifest.min-count-to-merge`, as in Java: how many manifests accumulate before a merge.
const MANIFEST_MIN_MERGE_COUNT: &str = "commit.manifest.min-count-to-merge";
const MANIFEST_MIN_MERGE_COUNT_DEFAULT: usize = 100;

struct RowDeltaOperation {
    replace: bool,
    /// Merge the carried delete manifests into one when at least this many have accumulated.
    merge_delete_manifests_at: Option<usize>,
    adds_data: bool,
    adds_deletes: bool,
    removed: HashSet<FileKey>,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    // As Java's BaseRowDelta: an append when only data is added, a delete when only deletes
    // are added, an overwrite otherwise.
    fn operation(&self) -> Operation {
        if self.replace {
            return Operation::Replace;
        }
        match (
            self.adds_data,
            self.adds_deletes || !self.removed.is_empty(),
        ) {
            (true, false) => Operation::Append,
            (false, true) => Operation::Delete,
            _ => Operation::Overwrite,
        }
    }

    fn truncate_full_table(&self) -> bool {
        false
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table = snapshot_produce.table;
        let mut manifests = vec![];
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(manifests);
        };
        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let removes_data = self.removed.iter().any(|k| k.1.is_none());
        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
                // Nothing live in it. As in Java's MergingSnapshotProducer, a manifest holding only
                // deleted entries belongs to the snapshot that deleted them and is not carried forward.
                continue;
            }
            // A manifest can only hold a removed file of its own content type.
            let relevant = !self.removed.is_empty()
                && (manifest_file.content == ManifestContentType::Deletes || removes_data);
            if !relevant {
                manifests.push(manifest_file.clone());
                continue;
            }
            let manifest = table.manifest_reader().read(manifest_file).await?;
            let hit = |e: &crate::spec::ManifestEntryRef| {
                e.is_alive() && self.removed.contains(&key_of(e.data_file()))
            };
            if !manifest.entries().iter().any(hit) {
                manifests.push(manifest_file.clone());
                continue;
            }
            let mut writer = snapshot_produce.new_manifest_writer(
                manifest_file.content,
                manifest.metadata().schema.clone(),
                Arc::new(manifest.metadata().partition_spec.clone()),
            )?;
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                if self.removed.contains(&key_of(entry.data_file())) {
                    writer.add_delete_entry(entry.as_ref().clone())?;
                } else {
                    writer.add_existing_entry(entry.as_ref().clone())?;
                }
            }
            manifests.push(writer.write_manifest_file().await?);
        }

        // Delete manifests accumulate one per commit. Past the table's threshold the carried ones
        // of the default spec are merged into one, as Java's MergingSnapshotProducer does.
        if let Some(threshold) = self.merge_delete_manifests_at {
            let spec_id = table.metadata().default_partition_spec_id();
            let mergeable = |m: &ManifestFile| {
                m.content == ManifestContentType::Deletes && m.partition_spec_id == spec_id
            };
            if manifests.iter().filter(|m| mergeable(m)).count() >= threshold.max(2) {
                let (to_merge, mut kept): (Vec<ManifestFile>, Vec<ManifestFile>) =
                    manifests.into_iter().partition(mergeable);
                let mut writer: Option<crate::spec::ManifestWriter> = None;
                for manifest_file in &to_merge {
                    let manifest = table.manifest_reader().read(manifest_file).await?;
                    if writer.is_none() {
                        writer = Some(snapshot_produce.new_manifest_writer(
                            ManifestContentType::Deletes,
                            manifest.metadata().schema.clone(),
                            Arc::new(manifest.metadata().partition_spec.clone()),
                        )?);
                    }
                    for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                        writer
                            .as_mut()
                            .unwrap()
                            .add_existing_entry(entry.as_ref().clone())?;
                    }
                }
                if let Some(writer) = writer {
                    kept.push(writer.write_manifest_file().await?);
                }
                manifests = kept;
            }
        }
        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Catalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v1_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};

    fn data_file(path: &str, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    /// A deletion vector: one blob, at `offset`, of a Puffin file.
    fn deletion_vector(puffin: &str, offset: i64, references: &str, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(puffin.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(500)
            .record_count(2)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(references.to_string()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap()
    }

    /// (manifest content, entry status, path, blob offset) of every entry of the current snapshot.
    async fn entries(
        table: &Table,
    ) -> Vec<(ManifestContentType, ManifestStatus, String, Option<i64>)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut out = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for e in manifest.entries() {
                out.push((
                    manifest_file.content,
                    e.status(),
                    e.file_path().to_string(),
                    e.data_file().content_offset,
                ));
            }
        }
        out
    }

    async fn delete_manifest_count(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count()
    }

    async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let tx = tx.fast_append().add_data_files(files).apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    #[tokio::test]
    async fn test_row_delta_commits_data_and_delete_files_in_one_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let table = append(&catalog, &table, vec![data_file("d/base.parquet", spec_id)]).await;

        let dv = deletion_vector("d/dv-1.puffin", 4, "d/base.parquet", spec_id);
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(vec![data_file("d/insert.parquet", spec_id)])
            .add_delete_files(vec![dv])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("added-data-files").unwrap(), "1");
        assert_eq!(summary.get("added-delete-files").unwrap(), "1");

        let all = entries(&table).await;
        assert!(all.contains(&(
            ManifestContentType::Deletes,
            ManifestStatus::Added,
            "d/dv-1.puffin".to_string(),
            Some(4)
        )));
        assert!(all.contains(&(
            ManifestContentType::Data,
            ManifestStatus::Added,
            "d/insert.parquet".to_string(),
            None
        )));
        // the base file is untouched: a row-level delete does not rewrite it
        assert!(
            all.iter()
                .any(|e| e.2 == "d/base.parquet" && e.1 != ManifestStatus::Deleted)
        );
    }

    #[tokio::test]
    async fn test_row_delta_operation_follows_what_was_added() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let table = append(&catalog, &table, vec![data_file("d/base.parquet", spec_id)]).await;

        // only delete files: a delete
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_delete_files(vec![deletion_vector(
                "d/dv-1.puffin",
                4,
                "d/base.parquet",
                spec_id,
            )])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );

        // only data files: an append
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(vec![data_file("d/more.parquet", spec_id)])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append
        );
    }

    #[tokio::test]
    async fn test_replacing_one_deletion_vector_leaves_its_puffin_neighbour() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let table = append(&catalog, &table, vec![
            data_file("d/a.parquet", spec_id),
            data_file("d/b.parquet", spec_id),
        ])
        .await;

        // two vectors in ONE Puffin file, told apart only by their blob offset
        let dv_a = deletion_vector("d/dv-1.puffin", 4, "d/a.parquet", spec_id);
        let dv_b = deletion_vector("d/dv-1.puffin", 44, "d/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_delete_files(vec![dv_a.clone(), dv_b])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // a.parquet is given a new vector; its old one is removed, b's is not
        let dv_a2 = deletion_vector("d/dv-2.puffin", 4, "d/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_delete_files(vec![dv_a2])
            .remove_delete_files(vec![dv_a])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let all = entries(&table).await;
        let status_of = |path: &str, offset: i64| {
            all.iter()
                .find(|e| e.2 == path && e.3 == Some(offset))
                .map(|e| e.1)
        };
        assert_eq!(status_of("d/dv-1.puffin", 4), Some(ManifestStatus::Deleted));
        assert_eq!(
            status_of("d/dv-1.puffin", 44),
            Some(ManifestStatus::Existing)
        );
        assert_eq!(status_of("d/dv-2.puffin", 4), Some(ManifestStatus::Added));
    }

    #[tokio::test]
    async fn test_delete_only_manifest_is_not_carried_into_the_next_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let old = data_file("d/old.parquet", spec_id);
        let table = append(&catalog, &table, vec![old.clone()]).await;

        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(vec![data_file("d/new.parquet", spec_id)])
            .remove_data_files(vec![old])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert!(
            entries(&table)
                .await
                .iter()
                .any(|e| e.2 == "d/old.parquet" && e.1 == ManifestStatus::Deleted),
            "the snapshot that removes a file records it as deleted"
        );

        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(vec![data_file("d/later.parquet", spec_id)])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert!(
            !entries(&table).await.iter().any(|e| e.2 == "d/old.parquet"),
            "the tombstone, and the column bounds it carries, do not outlive the snapshot that wrote it"
        );
    }

    #[tokio::test]
    async fn test_replace_removes_data_and_delete_files_in_one_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let base = data_file("d/base.parquet", spec_id);
        let table = append(&catalog, &table, vec![base.clone()]).await;
        let dv = deletion_vector("d/dv-1.puffin", 4, "d/base.parquet", spec_id);
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_delete_files(vec![dv.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let snapshots_before = table.metadata().snapshots().count();

        let tx = Transaction::new(&table);
        let tx = tx
            .replace_files()
            .remove_data_files(vec![base])
            .remove_delete_files(vec![dv])
            .add_data_files(vec![data_file("d/compacted.parquet", spec_id)])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(table.metadata().snapshots().count(), snapshots_before + 1);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        let live: Vec<String> = entries(&table)
            .await
            .into_iter()
            .filter(|e| e.1 != ManifestStatus::Deleted)
            .map(|e| e.2)
            .collect();
        assert_eq!(live, vec!["d/compacted.parquet".to_string()]);
    }

    #[tokio::test]
    async fn test_replace_must_remove_something() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let action = Transaction::new(&table)
            .replace_files()
            .add_data_files(vec![data_file("d/x.parquet", spec_id)]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_manifests_are_merged_at_the_table_threshold() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "3".to_string(),
            )
            .apply(tx)
            .unwrap();
        let mut table = tx.commit(&catalog).await.unwrap();
        table = append(&catalog, &table, vec![data_file("d/base.parquet", spec_id)]).await;

        let mut counts = vec![];
        for i in 0..5i64 {
            let tx = Transaction::new(&table);
            let tx = tx
                .row_delta()
                .add_delete_files(vec![deletion_vector(
                    &format!("d/dv-{i}.puffin"),
                    4,
                    "d/base.parquet",
                    spec_id,
                )])
                .apply(tx)
                .unwrap();
            table = tx.commit(&catalog).await.unwrap();
            counts.push(delete_manifest_count(&table).await);
        }
        // one per commit until three have been carried; then those three become one
        assert_eq!(counts, vec![1, 2, 3, 2, 3]);
        let live_vectors = entries(&table)
            .await
            .into_iter()
            .filter(|e| e.0 == ManifestContentType::Deletes && e.1 != ManifestStatus::Deleted)
            .count();
        assert_eq!(live_vectors, 5, "merging loses no delete file");
    }

    #[tokio::test]
    async fn test_delete_manifest_merge_can_be_switched_off() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "2".to_string(),
            )
            .set(
                "commit.manifest-merge.enabled".to_string(),
                "false".to_string(),
            )
            .apply(tx)
            .unwrap();
        let mut table = tx.commit(&catalog).await.unwrap();
        table = append(&catalog, &table, vec![data_file("d/base.parquet", spec_id)]).await;
        for i in 0..4i64 {
            let tx = Transaction::new(&table);
            let tx = tx
                .row_delta()
                .add_delete_files(vec![deletion_vector(
                    &format!("d/dv-{i}.puffin"),
                    4,
                    "d/base.parquet",
                    spec_id,
                )])
                .apply(tx)
                .unwrap();
            table = tx.commit(&catalog).await.unwrap();
        }
        assert_eq!(delete_manifest_count(&table).await, 4);
    }

    #[tokio::test]
    async fn test_row_delta_rejects_delete_files_on_format_version_1() {
        let table = make_v1_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let mut dv = deletion_vector("d/dv.puffin", 4, "d/base.parquet", spec_id);
        dv.partition = Struct::from_iter([Some(Literal::long(300))]);
        let action = Transaction::new(&table)
            .row_delta()
            .add_delete_files(vec![dv]);
        let err = Arc::new(action).commit(&table).await.err().unwrap();
        assert!(err.message().contains("format version 1"), "{err}");
    }

    #[tokio::test]
    async fn test_row_delta_rejects_a_data_file_passed_as_a_delete_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let action = Transaction::new(&table)
            .row_delta()
            .add_delete_files(vec![data_file("d/not-a-delete.parquet", spec_id)]);
        let err = Arc::new(action).commit(&table).await.err().unwrap();
        assert!(err.message().contains("Only delete content"), "{err}");
    }

    #[tokio::test]
    async fn test_row_delta_rejects_a_delete_file_passed_as_a_data_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let action = Transaction::new(&table)
            .row_delta()
            .add_data_files(vec![deletion_vector(
                "d/dv.puffin",
                4,
                "d/base.parquet",
                spec_id,
            )]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }
}
