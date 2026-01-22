"""Input validation utilities for ncdb-tools."""

from pathlib import Path
from typing import Optional, Sequence, Union


class NCDBValidationError(ValueError):
    """Raised when input validation fails."""

    pass


def validate_path(
    path: Union[str, Path],
    *,
    must_exist: bool = True,
    allowed_extensions: Optional[Sequence[str]] = None,
    description: str = "path",
) -> Path:
    """Validate and resolve a file path.

    Args:
        path: Path to validate
        must_exist: Whether the path must exist
        allowed_extensions: If provided, path must have one of these extensions
        description: Human-readable description for error messages

    Returns:
        Resolved Path object

    Raises:
        NCDBValidationError: If validation fails
    """
    try:
        resolved = Path(path).resolve()
    except (TypeError, ValueError) as e:
        raise NCDBValidationError(f"Invalid {description}: {e}") from e

    if must_exist and not resolved.exists():
        raise NCDBValidationError(f"{description.capitalize()} does not exist: {path}")

    if allowed_extensions:
        ext = resolved.suffix.lower()
        if ext not in [e.lower() for e in allowed_extensions]:
            allowed = ", ".join(allowed_extensions)
            raise NCDBValidationError(
                f"{description.capitalize()} must have extension: {allowed} (got {ext})"
            )

    return resolved


def validate_directory(
    path: Union[str, Path],
    *,
    must_exist: bool = True,
    create: bool = False,
    description: str = "directory",
) -> Path:
    """Validate a directory path.

    Args:
        path: Directory path to validate
        must_exist: Whether the directory must exist
        create: Whether to create the directory if it doesn't exist
        description: Human-readable description for error messages

    Returns:
        Resolved Path object

    Raises:
        NCDBValidationError: If validation fails
    """
    resolved = validate_path(path, must_exist=False, description=description)

    if resolved.exists() and not resolved.is_dir():
        raise NCDBValidationError(
            f"{description.capitalize()} is not a directory: {path}"
        )

    if not resolved.exists():
        if create:
            resolved.mkdir(parents=True, exist_ok=True)
        elif must_exist:
            raise NCDBValidationError(
                f"{description.capitalize()} does not exist: {path}"
            )

    return resolved


def validate_memory_limit(memory_limit: str) -> int:
    """Validate and parse a memory limit string.

    Args:
        memory_limit: Memory limit string (e.g., "4GB", "512MB")

    Returns:
        Memory limit in bytes

    Raises:
        NCDBValidationError: If format is invalid
    """
    if not isinstance(memory_limit, str):
        type_name = type(memory_limit).__name__
        raise NCDBValidationError(
            f"Memory limit must be a string (e.g., '4GB'), got {type_name}"
        )

    memory_limit = memory_limit.strip().upper()

    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    for suffix, multiplier in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if memory_limit.endswith(suffix):
            try:
                value = float(memory_limit[: -len(suffix)])
                if value <= 0:
                    raise NCDBValidationError(
                        f"Memory limit must be positive: {memory_limit}"
                    )
                return int(value * multiplier)
            except ValueError as e:
                raise NCDBValidationError(
                    f"Invalid memory limit format: {memory_limit}"
                ) from e

    raise NCDBValidationError(
        f"Memory limit must include unit (e.g., '4GB', '512MB'): {memory_limit}"
    )


def sanitize_path_for_logging(path: Path) -> str:
    """Return a sanitized path string safe for logging.

    Only includes the final path component to avoid leaking
    potentially sensitive directory structures.

    Args:
        path: Path to sanitize

    Returns:
        Sanitized string representation
    """
    return f".../{path.name}"
