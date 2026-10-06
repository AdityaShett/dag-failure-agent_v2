import difflib


def make_diff(source: str, fixed: str, path: str) -> str:
    return "\n".join(difflib.unified_diff(
        source.splitlines(), fixed.splitlines(), f"a/{path}", f"b/{path}", lineterm=""))


def diff_line_count(diff_text: str) -> int:
    return sum(
        1 for line in (diff_text or "").splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )