
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
OUTPUT_FILE = PROJECT_ROOT / "code.txt"

# Folders that are NOT part of the main project code
IGNORE_FOLDERS = {
    ".venv",
    "venv",
    "env",
    ".git",
    "__pycache__",
    "scripts",
    "tests",
    "test",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "build",
    "dist",
}

# Only include these file types
ALLOWED_EXTENSIONS = {
    ".py",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".ini",
    ".cfg",
    ".txt",
    ".md",
    ".dockerfile",
    ".sql",
}

# Files that should never be included
IGNORE_FILES = {
    "gather.py",
    "code.txt",
}


def main():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as output:

        for file in sorted(PROJECT_ROOT.rglob("*")):

            if not file.is_file():
                continue

            # Skip ignored folders
            if any(folder in IGNORE_FOLDERS for folder in file.parts):
                continue

            # Skip ignored files
            if file.name in IGNORE_FILES:
                continue

            # Only include approved file types
            if file.suffix.lower() not in ALLOWED_EXTENSIONS:
                continue

            relative_path = file.relative_to(PROJECT_ROOT)

            output.write("=" * 80 + "\n")
            output.write(f"FILE: {relative_path}\n")
            output.write("=" * 80 + "\n\n")

            try:
                output.write(file.read_text(encoding="utf-8"))
            except Exception:
                output.write("[Unable to read this file]\n")

            output.write("\n\n")

    print(f"Created {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
