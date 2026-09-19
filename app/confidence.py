import re

SIGNAL_NAMES = (
    "stack_trace_present",
    "line_number_matches_source",
    "known_fix_pattern_match",
    "log_completeness",
)

_TRACEBACK_HEADER = re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE)
_FRAME_RE = re.compile(r'File "(?P<file>[^"]+)", line (?P<line>\d+), in (?P<func>[^\s]+)')
_EXC_RE = re.compile(
    r"^(?:\[[^\]]*\]\s*)?(?:[A-Za-z_][\w.]*\s*-\s*)?"
    r"(?P<exc>(?:[a-zA-Z_][\w]*\.)*[A-Z][\w]*(?:Error|Exception|Timeout))"
    r"\s*:\s*(?P<msg>.*)$"
)
_LEVEL_RE = re.compile(r"\b(ERROR|CRITICAL|WARNING|INFO|DEBUG)\b")
_TIMESTAMP_RE = re.compile(r"\[\d{4}-\d{2}-\d{2}[ T,]")
_EXTERNAL_PATH_MARKERS = ("/site-packages/", "\\site-packages\\", "/dist-packages/")

KNOWN_FIX_PATTERNS = {
    "KeyError", "FileNotFoundError", "AttributeError", "ModuleNotFoundError",
    "ImportError", "NameError", "IndexError", "TypeError", "ValueError",
    "UnboundLocalError", "ZeroDivisionError",
}


def _strip_log_prefix(line: str) -> str:
    line = re.sub(r"^\ufeff", "", line)
    line = re.sub(r"^\[[^\]]*\]\s*", "", line)
    line = re.sub(r"^\{[^}]*\}\s*", "", line)
    line = re.sub(r"^(ERROR|CRITICAL|WARNING|INFO|DEBUG)\s*-\s*", "", line)
    return line


def _normalize_code(code: str) -> str:
    return re.sub(r"\s+", "", code or "")


def parse_log(task_logs: str) -> dict:
    text = task_logs or ""
    raw_lines = text.splitlines()
    lines = [_strip_log_prefix(l) for l in raw_lines]

    frames = []
    for i, line in enumerate(lines):
        m = _FRAME_RE.search(line)
        if not m:
            continue
        code = ""
        if i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt and not _FRAME_RE.search(nxt) and not _EXC_RE.match(nxt):
                code = nxt
        frames.append({"file": m.group("file"), "line": int(m.group("line")),
                        "func": m.group("func"), "code": code})

    exception_type = None
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = _EXC_RE.match(stripped)
        if m:
            exception_type = m.group("exc")
            break

    app_frame = None
    for frame in reversed(frames):
        if not any(marker in frame["file"] for marker in _EXTERNAL_PATH_MARKERS):
            app_frame = frame
            break

    return {
        "has_traceback": bool(_TRACEBACK_HEADER.search(text)),
        "frames": frames,
        "app_frame": app_frame,
        "exception_type": exception_type,
        "has_timestamps": bool(_TIMESTAMP_RE.search(text)),
        "has_level": bool(_LEVEL_RE.search(text)),
        "line_count": len([l for l in raw_lines if l.strip()]),
    }


def signal_stack_trace_present(parsed: dict) -> float:
    if parsed["has_traceback"] and parsed["frames"] and parsed["exception_type"]:
        return 1.0
    if parsed["frames"] and parsed["exception_type"]:
        return 0.8
    if parsed["exception_type"]:
        return 0.4
    return 0.0


def signal_line_number_matches_source(parsed: dict, dag_source: str, tolerance: int = 3) -> float:
    source = dag_source or ""
    if not source.strip():
        return 0.0

    frame = parsed.get("app_frame")
    if not frame:
        return 0.0

    src_lines = source.splitlines()
    n = len(src_lines)
    code = _normalize_code(frame.get("code", ""))

    if code:
        target = frame["line"]
        lo, hi = max(1, target - tolerance), min(n, target + tolerance)
        for idx in range(lo, hi + 1):
            if _normalize_code(src_lines[idx - 1]) == code:
                return 1.0
        for src_line in src_lines:
            if _normalize_code(src_line) == code:
                return 0.6
        if len(code) >= 12:
            for src_line in src_lines:
                if code in _normalize_code(src_line):
                    return 0.6
        return 0.0

    func = frame.get("func")
    in_range = 1 <= frame["line"] <= n
    func_present = bool(func) and (
        func == "<module>" or re.search(rf"\bdef\s+{re.escape(func)}\b", source)
    )
    return 0.25 if (in_range and func_present) else 0.0


def signal_known_fix_pattern_match(parsed: dict) -> float:
    exc = parsed.get("exception_type")
    if not exc:
        return 0.0
    short = exc.rsplit(".", 1)[-1]
    return 1.0 if short in KNOWN_FIX_PATTERNS else 0.0


def signal_log_completeness(parsed: dict, task_logs: str, dag_id: str = "", task_id: str = "") -> float:
    text = task_logs or ""
    checks = [
        parsed["has_timestamps"],
        parsed["has_level"],
        parsed["line_count"] >= 4,
        bool(task_id) and task_id in text,
        bool(dag_id) and dag_id in text,
        len(text) >= 200,
    ]
    return round(sum(1 for c in checks if c) / len(checks), 4)


def extract_signals(task_logs: str, dag_source: str, dag_id: str = "", task_id: str = "") -> dict:
    parsed = parse_log(task_logs)
    return {
        "stack_trace_present": signal_stack_trace_present(parsed),
        "line_number_matches_source": signal_line_number_matches_source(parsed, dag_source),
        "known_fix_pattern_match": signal_known_fix_pattern_match(parsed),
        "log_completeness": signal_log_completeness(parsed, task_logs, dag_id, task_id),
    }


def score(signals: dict, weights: dict) -> dict:
    numer = 0.0
    denom = 0.0
    contributions = {}
    for name in SIGNAL_NAMES:
        w = float(weights.get(name, 0.0))
        s = max(0.0, min(1.0, float(signals.get(name, 0.0))))
        contributions[name] = w * s
        numer += w * s
        denom += w

    value = max(0.0, min(1.0, numer / denom if denom > 0 else 0.0))
    total_contribution = sum(contributions.values()) or 1.0
    shares = {name: c / total_contribution for name, c in contributions.items()}

    return {"score": round(value, 4), "contributions": contributions, "shares": shares}
