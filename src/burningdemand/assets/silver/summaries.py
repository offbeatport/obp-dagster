# burningdemand_dagster/assets/summaries.py
import re
import pandas as pd
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


def clean_text_for_summarization(text: str, source: str) -> str:
    """Clean text before summarization based on source type."""
    if not text:
        return ""
    
    if source == "github":
        # Strip code blocks (```...``` or ```language...```)
        text = re.sub(r'```[\s\S]*?```', '', text)
        # Strip inline code blocks (`...`)
        text = re.sub(r'`[^`]+`', '', text)
        # Remove common log patterns (lines with timestamps, error traces, etc.)
        text = re.sub(r'^\d{4}-\d{2}-\d{2}.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\d{2}:\d{2}:\d{2}.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'(?i)(error|exception|traceback|stack trace).*', '', text, flags=re.MULTILINE)
    
    elif source in ("reddit", "hackernews"):
        # Remove quoted blocks (lines starting with >)
        text = re.sub(r'^>.*$', '', text, flags=re.MULTILINE)
        # Remove signatures (common patterns like "---", "---", email-like patterns at end)
        text = re.sub(r'\n---+\n.*$', '', text, flags=re.DOTALL)
        text = re.sub(r'\n--\s*\n.*$', '', text, flags=re.DOTALL)
        # Remove "edit:" and everything after it (case insensitive)
        text = re.sub(r'(?i)\n\s*edit[:\s].*$', '', text, flags=re.DOTALL)
        text = re.sub(r'(?i)^\s*edit[:\s].*$', '', text, flags=re.MULTILINE)
        # Remove email-like patterns at end (common in signatures)
        text = re.sub(r'\n\S+@\S+\.\S+.*$', '', text, flags=re.DOTALL)
    
    # Common cleanup for all sources
    # Remove excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    return text.strip()


def summarize_text(text: str, max_sentences: int = 3) -> str:
    """Extractive summarization using LexRank algorithm from sumy library."""
    if not text or len(text.strip()) < 50:
        return text[:500] if text else ""

    try:
        # Parse the text
        parser = PlaintextParser.from_string(text, Tokenizer("english"))

        # Use LexRank summarizer (graph-based algorithm using cosine similarity)
        summarizer = LexRankSummarizer()

        # Generate summary
        summary_sentences = summarizer(parser.document, max_sentences)

        # Join sentences
        summary = " ".join(str(sentence) for sentence in summary_sentences)

        # Limit to 1000 chars to avoid overly long summaries
        return summary[:1000] if summary else text[:500]
    except Exception:
        # Fallback to first 500 chars if summarization fails
        return text[:500]


@asset(
    partitions_def=daily_partitions,
    deps=[AssetKey(["silver", "clusters"])],
    description="Generate text summaries for each cluster using LexRank. Combines sample issue bodies and extracts key sentences to summarize the cluster's theme.",
)
def summaries(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key

    # Get clusters that don't have summaries yet
    clusters = db.query_df(
        """
        SELECT cluster_id, cluster_size
        FROM silver.clusters
        WHERE cluster_date = ?
          AND summary IS NULL
        """,
        [date],
    )

    if len(clusters) == 0:
        context.log.info(f"No new clusters to summarize for {date}")
        return MaterializeResult(metadata={"summarized": 0})

    cluster_ids = clusters["cluster_id"].astype(int).tolist()

    # Get titles, bodies, and source for each cluster
    items_df = db.query_df(
        f"""
        SELECT s.cluster_id, b.title, b.body, b.source
        FROM silver.items s
        JOIN bronze.raw_items b ON s.url_hash = b.url_hash
        WHERE s.cluster_date = ?
          AND s.cluster_id IN ({",".join(["?"] * len(cluster_ids))})
        """,
        [date, *cluster_ids],
    )

    summaries_data = []
    for cid in cluster_ids:
        cluster_items = items_df[items_df["cluster_id"] == cid].head(10)
        
        # Clean each body based on its source, then combine
        cleaned_bodies = []
        for _, row in cluster_items.iterrows():
            body = str(row.get("body", "") or "")
            source = str(row.get("source", "") or "github")
            if body and len(body.strip()) > 0:
                cleaned = clean_text_for_summarization(body, source)
                if cleaned and len(cleaned.strip()) > 0:
                    cleaned_bodies.append(cleaned)
        
        combined_body = " ".join(cleaned_bodies)
        summary = summarize_text(combined_body, max_sentences=3)

        summaries_data.append(
            {
                "cluster_date": date,
                "cluster_id": int(cid),
                "summary": summary,
            }
        )

    if summaries_data:
        summaries_df = pd.DataFrame(summaries_data)

        # Convert string columns to object dtype for DuckDB compatibility
        summaries_df["cluster_date"] = summaries_df["cluster_date"].astype("object")
        summaries_df["summary"] = summaries_df["summary"].astype("object")

        # Update summaries in clusters table
        for _, row in summaries_df.iterrows():
            db.execute(
                """
                UPDATE silver.clusters
                SET summary = ?
                WHERE cluster_date = ? AND cluster_id = ?
                """,
                [row["summary"], row["cluster_date"], row["cluster_id"]],
            )

    return MaterializeResult(metadata={"summarized": int(len(summaries_data))})
