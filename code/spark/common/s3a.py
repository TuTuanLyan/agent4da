"""Tiny S3A helper shared by Bronze and Silver."""


def s3a_options(minio):
    return {
        "spark.hadoop.fs.s3a.endpoint": minio.endpoint,
        "spark.hadoop.fs.s3a.access.key": minio.access_key,
        "spark.hadoop.fs.s3a.secret.key": minio.secret_key,
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


def apply_s3a_options(builder, minio):
    for key, value in s3a_options(minio).items():
        builder = builder.config(key, value)
    return builder
