#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::array::{BooleanArray, RecordBatch, TimestampMillisecondArray, UInt32Array};
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};
    use tokio::io::AsyncSeekExt;
    use tonbo::{
        record::{Record, RecordRef, Schema, Timestamp},
        timestamp::Ts,
        Decode, Encode, Record,
    };

    #[derive(Record, Debug, PartialEq)]
    pub struct Event {
        #[record(primary_key)]
        id: Timestamp,
        created_at: Timestamp,
        updated_at: Option<Timestamp>,
    }

    #[test]
    fn test_record_create() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event = Event {
            id: Timestamp::from(ts - 10),
            created_at: Timestamp::from(ts),
            updated_at: Some(Timestamp::from(ts + 100)),
        };
        assert_eq!(
            event,
            Event {
                id: Timestamp::from(ts - 10),
                created_at: Timestamp::from(ts),
                updated_at: Some(Timestamp::from(ts + 100)),
            }
        );
    }

    #[test]
    fn test_as_record_ref() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        {
            let event = Event {
                id: Timestamp::from(ts - 10),
                created_at: Timestamp::from(ts),
                updated_at: Some(Timestamp::from(ts + 100)),
            };
            let event_ref = event.as_record_ref();
            assert_eq!(
                event_ref,
                EventRef {
                    id: Timestamp::from(ts - 10),
                    created_at: Some(Timestamp::from(ts)),
                    updated_at: Some(Timestamp::from(ts + 100)),
                }
            );
        }
        {
            let event = Event {
                id: Timestamp::from(ts - 10),
                created_at: Timestamp::from(ts),
                updated_at: None,
            };
            let event_ref = event.as_record_ref();
            assert_eq!(
                event_ref,
                EventRef {
                    id: Timestamp::from(ts - 10),
                    created_at: Some(Timestamp::from(ts)),
                    updated_at: None,
                }
            );
        }
    }

    #[test]
    fn test_record_projection() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        {
            let event = Event {
                id: Timestamp::from(ts - 10),
                created_at: Timestamp::from(ts),
                updated_at: Some(Timestamp::from(ts + 100)),
            };
            let mut event_ref = event.as_record_ref();
            // arrow_schema will add _null and _ts columns, so the index should be start from 2
            event_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(&EventSchema {}.arrow_schema())
                    .unwrap(),
                vec![4],
            ));
            assert_eq!(
                event_ref,
                EventRef {
                    id: Timestamp::from(ts - 10),
                    created_at: None,
                    updated_at: Some(Timestamp::from(ts + 100)),
                }
            );
        }
        {
            let event = Event {
                id: Timestamp::from(ts - 10),
                created_at: Timestamp::from(ts),
                updated_at: None,
            };
            let mut event_ref = event.as_record_ref();
            // arrow_schema will add _null and _ts columns, so the index should be start from 2
            event_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(&EventSchema {}.arrow_schema())
                    .unwrap(),
                vec![2],
            ));
            assert_eq!(
                event_ref,
                EventRef {
                    id: Timestamp::from(ts - 10),
                    created_at: None,
                    updated_at: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_record_encode_and_decode() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event = Event {
            id: Timestamp::from(ts - 10),
            created_at: Timestamp::from(ts),
            updated_at: Some(Timestamp::from(ts + 100)),
        };
        let event_ref = event.as_record_ref();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        assert_eq!(event_ref.size(), 26);
        event_ref.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded_event = Event::decode(&mut cursor).await.unwrap();
        assert_eq!(event, decoded_event);
    }

    #[tokio::test]
    async fn test_record_from_record_batch() {
        {
            let record_batch = RecordBatch::try_new(
                Arc::new(
                    EventSchema {}
                        .arrow_schema()
                        .project(&[0, 1, 2, 3, 4])
                        .unwrap(),
                ),
                vec![
                    Arc::new(BooleanArray::from(vec![false, true, false, false])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                    Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])),
                    Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])),
                    Arc::new(TimestampMillisecondArray::from(vec![
                        Some(1),
                        None,
                        None,
                        Some(4),
                    ])),
                ],
            )
            .unwrap();

            let project_mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(EventSchema {}.arrow_schema())
                    .unwrap(),
                vec![0, 1, 2, 3, 4],
            );
            for i in 0..record_batch.num_rows() {
                let record_ref = EventRef::from_record_batch(
                    &record_batch,
                    i,
                    &project_mask,
                    EventSchema {}.arrow_schema(),
                );
                assert_eq!(
                    record_ref.key(),
                    Ts {
                        ts: (i as u32).into(),
                        value: Timestamp::from(i as i64 + 1),
                    }
                );
                if let Some(event_ref) = record_ref.get() {
                    assert_eq!(event_ref.id, Timestamp::from(i as i64 + 1));
                    assert_eq!(event_ref.created_at, Some(Timestamp::from(i as i64 + 1)));
                    if i == 2 {
                        assert_eq!(event_ref.updated_at, None);
                    } else {
                        assert_eq!(event_ref.updated_at, Some(Timestamp::from(i as i64 + 1)));
                    }
                } else {
                    assert_eq!(i, 1);
                }
            }
        }
    }
}
