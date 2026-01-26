-- 1. INITIALIZE NAMESPACES (SCHEMAS)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. BRONZE LAYER: Raw Data (Latest State)
-- Purpose: Ingested data with minimal transformation.
-- Space Saving: We use a Primary Key to allow Upserts.
CREATE TABLE IF NOT EXISTS bronze.items (
    id VARCHAR PRIMARY KEY,         -- Unique ID from the source
    content TEXT,                   -- Raw text/body
    metadata JSON,                  -- Extra fields
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. SILVER LAYER: Enriched & Processed
-- Purpose: Data cleaned and ready for ML/Analysis.
CREATE TABLE IF NOT EXISTS silver.embeddings (
    item_id VARCHAR PRIMARY KEY,    -- Foreign key to bronze.items
    vector FLOAT[],                 -- The actual embedding array
    model_name VARCHAR,             -- Model used (e.g., 'text-embedding-3-small')
    updated_at TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES bronze.items (id)
);

CREATE TABLE IF NOT EXISTS silver.clusters (
    item_id VARCHAR PRIMARY KEY,
    cluster_id INTEGER,
    topic_label VARCHAR,
    confidence DOUBLE,
    FOREIGN KEY (item_id) REFERENCES bronze.items (id)
);

-- 4. GOLD LAYER: Business Insights
-- Purpose: Final "Burning Demand" issues.
-- Note: Often created as a TABLE for performance or a VIEW for space-saving.
CREATE TABLE IF NOT EXISTS gold.issues (
    issue_id VARCHAR PRIMARY KEY,   -- Aggregated issue ID
    title VARCHAR,
    summary TEXT,
    severity_score FLOAT,
    is_active BOOLEAN DEFAULT TRUE,
    last_detected_at TIMESTAMP
);

-- Example of a Space-Saving Gold View
CREATE VIEW IF NOT EXISTS gold.view_active_issue_details AS
SELECT 
    i.id, 
    i.content, 
    c.topic_label, 
    c.confidence
FROM bronze.items i
JOIN silver.clusters c ON i.id = c.item_id
WHERE c.confidence > 0.8;