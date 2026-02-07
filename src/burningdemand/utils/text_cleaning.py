# burningdemand_dagster/utils/text_cleaning.py
import re


def clean_body(text: str, source: str) -> str:
    """Clean body text by removing boilerplate, quotes, logs, and large code blocks based on source type.
    source: asset key like gh_issues, gh_discussions, rd, so, hn.
    """
    if not text:
        return ""
    
    # GitHub sources (issues, discussions): code blocks, inline code, logs
    if source in ("gh_issues", "gh_discussions"):
        # Strip code blocks (```...``` or ```language...```)
        text = re.sub(r'```[\s\S]*?```', '', text)
        # Strip inline code blocks (`...`)
        text = re.sub(r'`[^`]+`', '', text)
        # Remove common log patterns (lines with timestamps, error traces, etc.)
        text = re.sub(r'^\d{4}-\d{2}-\d{2}.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\d{2}:\d{2}:\d{2}.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'(?i)(error|exception|traceback|stack trace).*', '', text, flags=re.MULTILINE)
    
    # Reddit and HackerNews: quoted blocks, signatures, edits
    elif source in ("rd", "hn"):
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
    
    # StackOverflow: code blocks like GitHub
    elif source == "so":
        # Strip code blocks (```...``` or ```language...```)
        text = re.sub(r'```[\s\S]*?```', '', text)
        # Strip inline code blocks (`...`)
        text = re.sub(r'`[^`]+`', '', text)
    
    # Common cleanup for all sources
    # Remove excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    return text.strip()
