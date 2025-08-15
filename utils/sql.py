from pathlib import Path


def read_sql(filename: str) -> str:
    """
    Reads a .sql file from the /queries folder in the project root.

    Args:
        filename (str): Name of the SQL file without extension.

    Returns:
        str: The SQL query as a string.

    Raises:
        FileNotFoundError: If the .sql file does not exist.
    """
    # Resolve the path to the project root (one level above this file's parent)
    project_root = Path(__file__).resolve().parents[1]

    # Ensure queries folder is located in project root
    queries_dir = project_root / "queries"

    if not queries_dir.exists():
        raise FileNotFoundError(f"Queries folder not found at {queries_dir}")

    # Build full path to the SQL file
    sql_path = queries_dir / f"{filename}.sql"

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    # Read and return the SQL query text
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()
