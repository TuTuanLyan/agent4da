"""S3A configuration helpers for Spark and MinIO."""


def build_s3a_config_dict(minio_config):
    return {
        "spark.hadoop.fs.s3a.endpoint": minio_config.endpoint,
        "spark.hadoop.fs.s3a.access.key": minio_config.access_key,
        "spark.hadoop.fs.s3a.secret.key": minio_config.secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.hadoop.fs.s3a.socket.timeout": "60000",
        "spark.hadoop.fs.s3a.threads.max": "20",
    }


def apply_s3a_configs(builder, minio_config):
    for key, value in build_s3a_config_dict(minio_config).items():
        builder = builder.config(key, value)
    return builder

