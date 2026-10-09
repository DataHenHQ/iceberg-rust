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

//! Writes format-version-3 deletion vectors: `deletion-vector-v1` blobs in a Puffin file, each
//! described by a position-delete [`DataFile`] that references one data file.

use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
use crate::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat};
use crate::{Error, ErrorKind, Result};

/// The reserved field id of the `_pos` metadata column, which a deletion vector's blob names.
const ROW_POSITION_FIELD_ID: i32 = 2147483545;
/// A Puffin file starts with a four-byte magic; uncompressed blobs follow it back to back.
const PUFFIN_MAGIC_LEN: i64 = 4;

/// Writes one Puffin file holding a deletion vector per given data file and returns the delete
/// files to commit. `positions` is the complete set of deleted row positions of that data file.
pub async fn write_deletion_vectors(
    file_io: &FileIO,
    puffin_path: impl Into<String>,
    vectors: Vec<(DataFile, Vec<u64>)>,
) -> Result<Vec<DataFile>> {
    let puffin_path = puffin_path.into();
    let output = file_io.new_output(puffin_path.clone())?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
    let mut offset = PUFFIN_MAGIC_LEN;
    let mut described = Vec::with_capacity(vectors.len());
    for (data_file, positions) in vectors {
        if data_file.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "A deletion vector must reference a data file",
            ));
        }
        let mut bitmap = RoaringTreemap::new();
        for p in positions {
            bitmap.insert(p);
        }
        let cardinality = bitmap.len();
        let bytes = DeleteVector::new(bitmap).serialize()?;
        let length = bytes.len() as i64;
        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(vec![ROW_POSITION_FIELD_ID])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(bytes)
            .properties(HashMap::from([
                (
                    "referenced-data-file".to_string(),
                    data_file.file_path().to_string(),
                ),
                ("cardinality".to_string(), cardinality.to_string()),
            ]))
            .build();
        writer.add(blob, CompressionCodec::None).await?;
        described.push((data_file, cardinality, offset, length));
        offset += length;
    }
    let file_size = writer.close().await?.size;
    described
        .into_iter()
        .map(|(data_file, cardinality, offset, length)| {
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(puffin_path.clone())
                .file_format(DataFileFormat::Puffin)
                .partition(data_file.partition().clone())
                .partition_spec_id(data_file.partition_spec_id)
                .record_count(cardinality)
                .file_size_in_bytes(file_size)
                .referenced_data_file(Some(data_file.file_path().to_string()))
                .content_offset(Some(offset))
                .content_size_in_bytes(Some(length))
                .build()
                .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))
        })
        .collect()
}

/// Reads the deleted row positions a deletion-vector delete file records.
pub async fn read_deletion_vector(file_io: &FileIO, delete_file: &DataFile) -> Result<Vec<u64>> {
    let (Some(offset), Some(length)) = (
        delete_file.content_offset,
        delete_file.content_size_in_bytes,
    ) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Not a deletion vector: the delete file has no content offset and size",
        ));
    };
    let input = file_io.new_input(delete_file.file_path())?;
    let bytes = input
        .reader()
        .await?
        .read(offset as u64..(offset + length) as u64)
        .await?;
    Ok(DeleteVector::deserialize(&bytes)?.iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Literal, Struct};

    fn data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1000)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(7))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_written_vectors_read_back_and_describe_their_blobs() {
        let file_io = FileIO::new_with_memory();
        let vectors = vec![
            (data_file("memory:///t/data/a.parquet"), vec![
                3u64, 1, 500, 3,
            ]),
            (data_file("memory:///t/data/b.parquet"), vec![
                (1u64 << 33) + 5,
                0,
            ]),
        ];
        let written = write_deletion_vectors(&file_io, "memory:///t/data/dv-1.puffin", vectors)
            .await
            .unwrap();
        assert_eq!(written.len(), 2);

        let (a, b) = (&written[0], &written[1]);
        for (dv, references) in [
            (a, "memory:///t/data/a.parquet"),
            (b, "memory:///t/data/b.parquet"),
        ] {
            assert_eq!(dv.content_type(), DataContentType::PositionDeletes);
            assert_eq!(dv.file_format(), DataFileFormat::Puffin);
            assert_eq!(dv.file_path(), "memory:///t/data/dv-1.puffin");
            assert_eq!(dv.referenced_data_file().as_deref(), Some(references));
            assert_eq!(dv.partition(), &Struct::from_iter([Some(Literal::long(7))]));
        }
        // duplicates collapse; the count is the vector's cardinality
        assert_eq!(a.record_count(), 3);
        assert_eq!(b.record_count(), 2);
        // blobs follow the four-byte magic, back to back
        assert_eq!(a.content_offset, Some(4));
        assert_eq!(b.content_offset, Some(4 + a.content_size_in_bytes.unwrap()));

        assert_eq!(read_deletion_vector(&file_io, a).await.unwrap(), vec![
            1, 3, 500
        ]);
        assert_eq!(read_deletion_vector(&file_io, b).await.unwrap(), vec![
            0,
            (1u64 << 33) + 5
        ]);
    }

    #[tokio::test]
    async fn test_a_vector_is_updated_by_writing_the_union_of_old_and_new_positions() {
        // Format version 3 allows one vector per data file: an update reads the old one, adds
        // the newly deleted positions, and writes a replacement.
        let file_io = FileIO::new_with_memory();
        let file = data_file("memory:///t/data/a.parquet");
        let first = write_deletion_vectors(&file_io, "memory:///t/data/dv-1.puffin", vec![(
            file.clone(),
            vec![10, 20],
        )])
        .await
        .unwrap()
        .remove(0);

        let mut positions = read_deletion_vector(&file_io, &first).await.unwrap();
        positions.extend([20, 30]);
        let second = write_deletion_vectors(&file_io, "memory:///t/data/dv-2.puffin", vec![(
            file, positions,
        )])
        .await
        .unwrap()
        .remove(0);

        assert_eq!(
            read_deletion_vector(&file_io, &second).await.unwrap(),
            vec![10, 20, 30]
        );
        assert_eq!(second.record_count(), 3);
        // the first vector is untouched: replacing it is a commit, not an overwrite of its file
        assert_eq!(read_deletion_vector(&file_io, &first).await.unwrap(), vec![
            10, 20
        ]);
    }

    #[tokio::test]
    async fn test_a_vector_must_reference_a_data_file() {
        let file_io = FileIO::new_with_memory();
        let dv = write_deletion_vectors(&file_io, "memory:///t/data/dv-1.puffin", vec![(
            data_file("memory:///t/data/a.parquet"),
            vec![1],
        )])
        .await
        .unwrap()
        .remove(0);
        let err =
            write_deletion_vectors(&file_io, "memory:///t/data/dv-2.puffin", vec![(dv, vec![
                1,
            ])])
            .await
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_reading_a_position_delete_file_as_a_vector_is_refused() {
        let file_io = FileIO::new_with_memory();
        let mut not_a_vector = data_file("memory:///t/data/deletes.parquet");
        not_a_vector.content = DataContentType::PositionDeletes;
        let err = read_deletion_vector(&file_io, &not_a_vector)
            .await
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
