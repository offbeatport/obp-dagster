"""Generic text utilities."""


def truncate_middle(text: str, max_len: int) -> str:
    """Truncate to max_len, keeping first and last parts."""
    if not text or len(text) <= max_len:
        return text or ""
    half = (max_len - 3) // 2
    return text[:half] + "..." + text[-half:]
