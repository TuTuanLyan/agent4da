# Gold Job Refactor

Gold hiện chạy qua một entrypoint duy nhất:

```bash
code/spark/gold_job.py
```

Airflow `gold_pipeline.py` chỉ gọi entrypoint này. Các Gold stage jobs cũ đã
được archive trong `code/spark/_archive/`.

## Module Layout

- `code/spark/common/config.py`: load config từ env, validate run mode.
- `code/spark/common/spark_session.py`: tạo SparkSession dùng chung.
- `code/spark/common/s3a.py`: Spark S3A/MinIO config.
- `code/spark/common/iceberg.py`: Iceberg catalog helper.
- `code/spark/common/data_quality.py`: helper nhỏ cho transform.
- `code/spark/gold/schemas.py`: định nghĩa Gold và metadata tables.
- `code/spark/gold/ddl.py`: build và chạy DDL từ schema definitions.
- `code/spark/gold/readers.py`: đọc Silver, cast, filter valid, deduplicate.
- `code/spark/gold/builders_mvp.py`: build MVP fact/dim/summary tables.
- `code/spark/gold/builders_extended.py`: build extended Gold tables.
- `code/spark/gold/builders_metadata.py`: build Agent metadata catalog.
- `code/spark/gold/writers.py`: write Iceberg tables theo refresh mode.
- `code/spark/gold/validators.py`: validate namespaces, tables, counts, samples.

## Run Modes

- `all`: tạo schema, build MVP, extended và metadata.
- `schema_only`: chỉ tạo Iceberg namespaces/tables rồi validate.
- `mvp_only`: build và ghi MVP tables.
- `extended_only`: build và ghi extended tables.
- `metadata_only`: build và ghi metadata catalog.
- `validate_only`: chỉ validate Iceberg outputs hiện có.

## Refresh Modes

- `full_refresh`: ưu tiên `INSERT OVERWRITE`; fallback `DELETE` rồi append nếu cần.
- `append`: append trực tiếp, có thể trùng dữ liệu nếu chạy lại cùng Silver input.

## Notes

- DDL dài không còn nằm trong `gold_job.py`.
- Business schema, table names, partitions và table properties được giữ nguyên.
- Metadata catalog vẫn được build từ rows tĩnh trong Python để dễ đọc và sửa.

