"""Parse the PPD data-codebook CSV into a variable dictionary."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

from pension_data.sources.ppd.variables import PPD_VARIABLES

# Column-name candidates seen across PPD codebook exports (case-insensitive).
_NAME_COLUMNS = ("variable", "variablename", "name", "field", "varname")
_DESCRIPTION_COLUMNS = ("description", "label", "definition", "desc")


@dataclass(frozen=True, slots=True)
class CodebookEntry:
    """One codebook variable with its human description."""

    name: str
    description: str


def _pick_column(fieldnames: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {name.strip().lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def parse_codebook_csv(body: str) -> dict[str, CodebookEntry]:
    """Parse a codebook CSV body into ``{variable_name: CodebookEntry}``.

    Tolerant of column naming: the variable-name and description columns are
    resolved from a set of known header spellings.
    """
    reader = csv.DictReader(io.StringIO(body))
    if reader.fieldnames is None:
        return {}
    name_col = _pick_column(list(reader.fieldnames), _NAME_COLUMNS)
    if name_col is None:
        raise ValueError(
            f"codebook CSV had no recognizable variable-name column in {reader.fieldnames}"
        )
    desc_col = _pick_column(list(reader.fieldnames), _DESCRIPTION_COLUMNS)

    entries: dict[str, CodebookEntry] = {}
    for row in reader:
        raw_name = (row.get(name_col) or "").strip()
        if not raw_name:
            continue
        description = (row.get(desc_col) or "").strip() if desc_col else ""
        entries[raw_name] = CodebookEntry(name=raw_name, description=description)
    return entries


def missing_mapped_variables(codebook: dict[str, CodebookEntry]) -> tuple[str, ...]:
    """Return mapped PPD variable names that are absent from a parsed codebook.

    Useful as a drift check: if a GUESS name in :data:`PPD_VARIABLES` is missing
    from a live codebook, this surfaces it so the one-line rename can be made.
    """
    mapped = {var.name for var in PPD_VARIABLES.values()}
    return tuple(sorted(name for name in mapped if name not in codebook))
