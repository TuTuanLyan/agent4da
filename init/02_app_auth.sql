-- Agent4DA app backend auth schema.
-- Docker Postgres runs this file only when the database volume is initialized.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.users (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'user',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT users_role_check CHECK (role IN ('user', 'admin'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_app_users_email_lower
  ON app.users (lower(email));

CREATE TABLE IF NOT EXISTS app.refresh_tokens (
  jti TEXT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_app_refresh_tokens_user_created
  ON app.refresh_tokens(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_app_refresh_tokens_active_expires
  ON app.refresh_tokens(expires_at)
  WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS app.user_preferences (
  user_id UUID PRIMARY KEY REFERENCES app.users(id) ON DELETE CASCADE,
  theme TEXT NOT NULL DEFAULT 'system',
  default_chart_type TEXT NOT NULL DEFAULT 'auto',
  default_model TEXT,
  preferred_language TEXT NOT NULL DEFAULT 'vi',
  export_delimiter TEXT NOT NULL DEFAULT ',',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
