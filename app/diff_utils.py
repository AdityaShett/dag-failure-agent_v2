import re
from unidiff import PatchSet


def sanitize_diff_text(diff_text: str) -> str:
    if not diff_text:
        return diff_text

    text = diff_text.strip()

    while True:
        new_text = re.sub(r"^```(?:diff|patch)?\s*\n", "", text)
        if new_text == text:
            break
        text = new_text

    while True:
        new_text = re.sub(r"\n?```\s*$", "", text)
        if new_text == text:
            break
        text = new_text

    lines = text.split("\n")
    repaired = []
    in_hunk = False
    for line in lines:
        if line.startswith("@@"):
            in_hunk = True
            repaired.append(line)
            continue
        if in_hunk and line == "":
            repaired.append(" ")
        else:
            repaired.append(line)
    return "\n".join(repaired)


def _locate_hunk(lines, expected, hint_start):
    n = len(expected)
    if n == 0:
        return hint_start
    expected_norm = [e.rstrip("\n") for e in expected]

    candidate = lines[hint_start:hint_start + n]
    if [l.rstrip("\n") for l in candidate] == expected_norm:
        return hint_start

    for start in range(len(lines) - n + 1):
        window = [l.rstrip("\n") for l in lines[start:start + n]]
        if window == expected_norm:
            return start

    return None


def apply_unified_diff(original_content: str, diff_text: str) -> str:
    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content

    diff_text = sanitize_diff_text(diff_text)

    patch = PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in patch:
        for hunk in patched_file:
            hint_start = hunk.source_start - 1
            expected = [line.value for line in hunk if not line.is_added]

            start = _locate_hunk(lines, expected, hint_start)
            if start is None:
                raise ValueError(
                    f"diff context not found (hunk claimed line {hunk.source_start})"
                )

            new_lines = [line.value for line in hunk if not line.is_removed]
            lines[start:start + hunk.source_length] = new_lines

    return "".join(lines)


def diff_line_count(diff_text: str) -> int:
    try:
        patch = PatchSet(sanitize_diff_text(diff_text))
    except Exception:
        return 0
    count = 0
    for patched_file in patch:
        for hunk in patched_file:
            for line in hunk:
                if line.is_added or line.is_removed:
                    count += 1
    return count
