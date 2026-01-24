-- burningdemand_dagster/sql/schema.sql

CREATE TABLE IF NOT EXISTS raw_items (
    url_hash        VARCHAR PRIMARY KEY,
    source          VARCHAR,
    collection_date DATE,
    url             VARCHAR UNIQUE,
    title           VARCHAR,
    body            VARCHAR,
    created_at      TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_items_date
    ON raw_items(collection_date);

CREATE INDEX IF NOT EXISTS idx_raw_items_source_date
    ON raw_items(source, collection_date);

CREATE TABLE IF NOT EXISTS embeddings (
    url_hash        VARCHAR PRIMARY KEY,
    embedding       FLOAT[384],
    embedding_date  DATE
);

CREATE INDEX IF NOT EXISTS idx_embeddings_date
    ON embeddings(embedding_date);

CREATE TABLE IF NOT EXISTS clusters (
    cluster_date DATE,
    cluster_id   INTEGER,
    cluster_size INTEGER,
    PRIMARY KEY (cluster_date, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_clusters_date
    ON clusters(cluster_date);

CREATE TABLE IF NOT EXISTS cluster_members (
    cluster_date DATE,
    cluster_id   INTEGER,
    url_hash     VARCHAR,
    PRIMARY KEY (cluster_date, cluster_id, url_hash)
);

CREATE INDEX IF NOT EXISTS idx_cluster_members_date
    ON cluster_members(cluster_date, cluster_id);

CREATE TABLE IF NOT EXISTS issue_canonical (
    cluster_date     DATE,
    cluster_id       INTEGER,
    canonical_title  VARCHAR,
    category         VARCHAR,
    description      VARCHAR,
    would_pay_signal BOOLEAN,
    impact_level     VARCHAR,
    cluster_size     INTEGER,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_issues_date
    ON issues(cluster_date);

CREATE TABLE IF NOT EXISTS issue_evidence (
    cluster_date DATE,
    cluster_id   INTEGER,
    url_hash     VARCHAR,
    source       VARCHAR,
    url          VARCHAR,
    title        VARCHAR,
    body         VARCHAR,
    posted_at    TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id, url_hash)
);

CREATE INDEX IF NOT EXISTS idx_issue_evidence_issue
    ON issue_evidence(cluster_date, cluster_id);

CREATE TABLE IF NOT EXISTS pocketbase_sync (
    cluster_date DATE,
    cluster_id   INTEGER,
    issue_id     VARCHAR,
    synced_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_pb_sync_date
    ON pocketbase_sync(cluster_date);
