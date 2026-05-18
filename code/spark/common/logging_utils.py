"""Small logging helpers that avoid leaking secrets."""


def log(component, message):
    print(f"[{component}] {message}", flush=True)


def mask_secret(value):
    if not value:
        return ""
    return "****"


def log_gold_config(config):
    log("GoldJob", f"Catalog: {config.catalog_name}")
    log("GoldJob", f"Gold namespace: {config.gold_namespace}")
    log("GoldJob", f"Metadata namespace: {config.metadata_namespace}")
    log("GoldJob", f"Warehouse: {config.warehouse}")
    log("GoldJob", f"JDBC URI: {config.jdbc_uri}")
    log("GoldJob", f"JDBC user: {config.jdbc_user}")
    log("GoldJob", f"JDBC password: {mask_secret(config.jdbc_password)}")
    log("GoldJob", f"JDBC schema: {config.jdbc_schema}")
    log("GoldJob", f"Silver path: {config.silver_events_path}")
    log("GoldJob", f"Run mode: {config.run_mode}")
    log("GoldJob", f"Refresh mode: {config.refresh_mode}")
    log("GoldJob", f"Dry run: {config.dry_run}")
    log("GoldJob", f"Validate tables: {config.validate_tables}")

