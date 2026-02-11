-- 1. INITIALIZE NAMESPACES
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. BRONZE LAYER
CREATE TABLE IF NOT EXISTS bronze.raw_items (
    url                 VARCHAR,
    url_hash            VARCHAR,
    source              VARCHAR,
    source_post_id      VARCHAR,
    post_type           VARCHAR,
    org_name            VARCHAR,
    product_name        VARCHAR,
    product_desc        VARCHAR,
    product_stars       INTEGER,
    product_forks       INTEGER,
    product_watchers    INTEGER,
    license             VARCHAR,
    labels              VARCHAR[],
    title               VARCHAR,
    body                VARCHAR,
    upvotes_count       INTEGER,
    reactions_groups    JSON,
    reactions_count     INTEGER,
    comments_list       JSON,
    comments_count      INTEGER,
    created_at          TIMESTAMP,
    collected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, post_type, source_post_id)
);


-- 3. SILVER LAYER
CREATE TABLE IF NOT EXISTS silver.classifications (
    url_hash            VARCHAR,
    classification_date DATE,
    pain_prob           DOUBLE,
    would_pay_prob      DOUBLE,
    noise_prob          DOUBLE,
    confidence          DOUBLE,
    language            VARCHAR,
    PRIMARY KEY (url_hash, classification_date)
);

CREATE TABLE IF NOT EXISTS silver.embeddings (
    url_hash        VARCHAR PRIMARY KEY,
    embedding       FLOAT[1024],
    embedding_date  DATE
);

CREATE TABLE IF NOT EXISTS silver.groups (
    group_date   DATE,
    group_id     INTEGER,
    group_size   INTEGER,
    outlier_ratio  DOUBLE,
    mean_distance  DOUBLE,
    authority_score DOUBLE,
    PRIMARY KEY (group_date, group_id)
);

CREATE TABLE IF NOT EXISTS silver.group_members (
    url_hash       VARCHAR,
    group_date   DATE,
    group_id     INTEGER,
    PRIMARY KEY (url_hash, group_date)
);


-- 4. GOLD LAYER
CREATE TABLE IF NOT EXISTS gold.issues (
    group_date           DATE,
    group_id             INTEGER,
    canonical_title        VARCHAR,
    category               VARCHAR,
    desc_problem           VARCHAR,
    desc_current_solutions VARCHAR,
    desc_impact            VARCHAR,
    desc_details           VARCHAR,
    would_pay_signal       BOOLEAN,
    impact_level           VARCHAR,
    group_size           INTEGER,
    authority_score        DOUBLE,
    label_failed           BOOLEAN DEFAULT FALSE,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_date, group_id)
);

CREATE TABLE IF NOT EXISTS gold.issue_evidence (
    group_date DATE,
    group_id   INTEGER,
    url_hash     VARCHAR,
    source       VARCHAR,
    url          VARCHAR,
    title        VARCHAR,
    body         VARCHAR,
    posted_at    TIMESTAMP,
    PRIMARY KEY (group_date, group_id, url_hash)
);

