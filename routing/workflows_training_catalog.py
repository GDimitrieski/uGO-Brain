from __future__ import annotations

import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from world.lab_world import ProcessType


@dataclass(frozen=True)
class TrainingWorkflowEntry:
    site: str
    sample_label: str
    cap_color: str
    material_code: str
    article_number: str
    steps_text: Tuple[str, ...]
    process_steps: Tuple[ProcessType, ...]
    metadata: Dict[str, str]


@dataclass(frozen=True)
class SampleTypeWorkflowProfile:
    sample_type_key: str
    display_name: str
    material_codes: Tuple[str, ...]
    article_numbers: Tuple[str, ...]
    cap_colors: Tuple[str, ...]
    canonical_process_steps: Tuple[ProcessType, ...]
    process_step_variants: Tuple[Tuple[ProcessType, ...], ...]
    evidence_count: int
    source_sites: Tuple[str, ...]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = text.replace("\u00d8", "O")
    return text


def _norm_token(value: str) -> str:
    txt = _normalize_text(value).upper()
    txt = re.sub(r"\s+", "", txt)
    return txt


def _parse_steps_to_processes(steps: Sequence[str]) -> Tuple[ProcessType, ...]:
    out: List[ProcessType] = []
    seen: set[ProcessType] = set()

    def add(proc: ProcessType) -> None:
        if proc in seen:
            return
        out.append(proc)
        seen.add(proc)

    for raw in steps:
        s = raw.lower()
        if "decap" in s:
            add(ProcessType.DECAP)
        if "cap" in s and "decap" not in s:
            add(ProcessType.CAP)
        if "zentrifug" in s or "centrifug" in s:
            add(ProcessType.CENTRIFUGATION)
        if "identifikation" in s or "barcode" in s:
            add(ProcessType.SAMPLE_TYPE_DETECTION)
        if "immun" in s:
            add(ProcessType.IMMUNOANALYSIS)
        if "archiv" in s:
            add(ProcessType.ARCHIVATION)
    return tuple(out)


def _profile_key(entry: TrainingWorkflowEntry) -> str:
    mat = _norm_token(entry.material_code)
    if mat:
        return f"MAT:{mat}"
    art = _norm_token(entry.article_number)
    if art:
        return f"ART:{art}"
    label = _norm_token(entry.sample_label)
    if label:
        return f"LBL:{label}"
    # Fallback should be extremely rare.
    return "LBL:UNKNOWN"


def _read_xlsx_rows(path: Path) -> Dict[str, List[List[str]]]:
    ns = {
        "x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    out: Dict[str, List[List[str]]] = {}
    with zipfile.ZipFile(path, "r") as z:
        shared: List[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            shared_root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in shared_root.findall("x:si", ns):
                parts = [(_normalize_text(t.text)) for t in si.findall(".//x:t", ns)]
                shared.append("".join(parts))

        wb = ET.fromstring(z.read("xl/workbook.xml"))
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        rel_map: Dict[str, str] = {}
        for rel in rels.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
            rid = rel.attrib.get("Id")
            target = rel.attrib.get("Target")
            if rid and target:
                rel_map[rid] = target

        for sheet in wb.findall("x:sheets/x:sheet", ns):
            name = sheet.attrib.get("name")
            rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if not name or not rid:
                continue
            target = rel_map.get(rid)
            if not target:
                continue
            sheet_path = "xl/" + target if not target.startswith("xl/") else target
            root = ET.fromstring(z.read(sheet_path))
            rows_raw = root.findall(".//x:sheetData/x:row", ns)

            rows: List[List[str]] = []
            for row in rows_raw:
                cells = row.findall("x:c", ns)
                vals: List[str] = []
                for c in cells:
                    t = c.attrib.get("t")
                    v = c.find("x:v", ns)
                    val = ""
                    if v is not None and v.text is not None:
                        raw = v.text
                        if t == "s":
                            try:
                                val = shared[int(raw)]
                            except Exception:
                                val = raw
                        else:
                            val = raw
                    else:
                        inline = c.find("x:is/x:t", ns)
                        if inline is not None and inline.text is not None:
                            val = inline.text
                    vals.append(_normalize_text(val))
                rows.append(vals)
            out[name] = rows
    return out


def _pick_header_index(rows: Sequence[Sequence[str]]) -> Optional[int]:
    for idx, row in enumerate(rows):
        for cell in row:
            if _normalize_text(cell).lower().startswith("schritt 1"):
                return idx
    return None


def _detect_row_offset(rows: Sequence[Sequence[str]], header_idx: int) -> int:
    check_rows = rows[header_idx + 1 : header_idx + 8]
    for row in check_rows:
        if not row:
            continue
        first = _normalize_text(row[0]).lower()
        if first.startswith("phase"):
            return 1
    return 0


def _step_columns(headers: Sequence[str]) -> List[int]:
    cols: List[int] = []
    for idx, h in enumerate(headers):
        txt = _normalize_text(h).lower()
        if txt.startswith("schritt"):
            cols.append(idx)
    return cols


def _find_col(headers: Sequence[str], *tokens: str) -> Optional[int]:
    lowered = [_normalize_text(h).lower() for h in headers]
    for idx, h in enumerate(lowered):
        if all(tok.lower() in h for tok in tokens):
            return idx
    return None


def _cell(row: Sequence[str], idx: Optional[int]) -> str:
    if idx is None:
        return ""
    if idx < 0 or idx >= len(row):
        return ""
    return _normalize_text(row[idx])


def _is_data_row(row: Sequence[str], step_cols: Sequence[int]) -> bool:
    if any(_normalize_text(x) for x in row[:6]):
        return True
    for c in step_cols:
        if c < len(row) and _normalize_text(row[c]):
            return True
    return False


def _is_specific_id_token(value: str) -> bool:
    token = _norm_token(value)
    if not token:
        return False
    # Material/article identifiers should carry at least one digit.
    return any(ch.isdigit() for ch in token)


def _is_digit_token(value: str) -> bool:
    token = _norm_token(value)
    return bool(token) and token.isdigit()


def _endswith_token_boundary(text: str, token: str) -> bool:
    if not text.endswith(token):
        return False
    if len(text) == len(token):
        return True
    prev = text[-len(token) - 1]
    if token.isdigit():
        return not prev.isdigit()
    return not prev.isalnum()


def _contains_token_boundary(text: str, token: str) -> bool:
    if not token:
        return False
    if token.isdigit():
        # For pure numeric tokens, avoid fuzzy substring matches (e.g., 99 in 999).
        pattern = rf"(^|\D){re.escape(token)}(\D|$)"
    else:
        pattern = rf"(^|[^A-Z0-9]){re.escape(token)}([^A-Z0-9]|$)"
    return re.search(pattern, text) is not None


def load_training_workflow_entries(
    path: Path,
) -> List[TrainingWorkflowEntry]:
    rows_by_sheet = _read_xlsx_rows(path)
    entries: List[TrainingWorkflowEntry] = []

    for site, rows in rows_by_sheet.items():
        header_idx = _pick_header_index(rows)
        if header_idx is None:
            continue
        headers = rows[header_idx]
        row_offset = _detect_row_offset(rows, header_idx)
        step_cols = _step_columns(headers)

        col_sample = _find_col(headers, "monovetten")
        col_cap = _find_col(headers, "kappenfarbe")
        col_material = _find_col(headers, "materialnummer")
        if col_material is None:
            col_material = _find_col(headers, "materialkennung")
        col_article = _find_col(headers, "artikelnummer")
        if col_article is None:
            col_article = _find_col(headers, "bestell")

        for raw_row in rows[header_idx + 1 :]:
            row = list(raw_row[row_offset:]) if row_offset > 0 else list(raw_row)
            if not _is_data_row(row, step_cols):
                continue
            steps = tuple(_cell(row, c) for c in step_cols if _cell(row, c) and _cell(row, c).lower() != "x")
            sample_label = _cell(row, col_sample)
            cap_color = _cell(row, col_cap)
            material = _cell(row, col_material)
            article = _cell(row, col_article)

            # If sample label is missing, fall back to first non-empty cell.
            if not sample_label:
                for v in row:
                    txt = _normalize_text(v)
                    if txt:
                        sample_label = txt
                        break

            if not sample_label and not material and not article and not steps:
                continue

            metadata: Dict[str, str] = {}
            for idx, head in enumerate(headers):
                key = _normalize_text(head)
                if not key:
                    continue
                val = _cell(row, idx)
                if val:
                    metadata[key] = val

            entries.append(
                TrainingWorkflowEntry(
                    site=site,
                    sample_label=sample_label,
                    cap_color=cap_color,
                    material_code=material,
                    article_number=article,
                    steps_text=steps,
                    process_steps=_parse_steps_to_processes(steps),
                    metadata=metadata,
                )
            )
    return entries


def build_sample_type_profiles(entries: Sequence[TrainingWorkflowEntry]) -> List[SampleTypeWorkflowProfile]:
    grouped: Dict[str, List[TrainingWorkflowEntry]] = {}
    for entry in entries:
        key = _profile_key(entry)
        grouped.setdefault(key, []).append(entry)

    profiles: List[SampleTypeWorkflowProfile] = []
    for key, items in grouped.items():
        material_codes = tuple(sorted({x.material_code for x in items if _normalize_text(x.material_code)}))
        article_numbers = tuple(sorted({x.article_number for x in items if _normalize_text(x.article_number)}))
        cap_colors = tuple(sorted({x.cap_color for x in items if _normalize_text(x.cap_color)}))
        source_sites = tuple(sorted({x.site for x in items if _normalize_text(x.site)}))

        variant_counts: Dict[Tuple[ProcessType, ...], int] = {}
        for item in items:
            variant = tuple(item.process_steps)
            variant_counts[variant] = variant_counts.get(variant, 0) + 1

        sorted_variants = sorted(
            variant_counts.items(),
            key=lambda kv: (kv[1], len(kv[0])),
            reverse=True,
        )
        canonical_process_steps = sorted_variants[0][0] if sorted_variants else ()
        process_step_variants = tuple(v for v, _ in sorted_variants)

        display_name = ""
        for item in items:
            if _normalize_text(item.sample_label):
                display_name = item.sample_label
                break
        if not display_name:
            display_name = key

        profiles.append(
            SampleTypeWorkflowProfile(
                sample_type_key=key,
                display_name=display_name,
                material_codes=material_codes,
                article_numbers=article_numbers,
                cap_colors=cap_colors,
                canonical_process_steps=canonical_process_steps,
                process_step_variants=process_step_variants,
                evidence_count=len(items),
                source_sites=source_sites,
            )
        )

    profiles.sort(key=lambda p: p.sample_type_key)
    return profiles


def load_training_workflow_profiles(path: Path) -> List[SampleTypeWorkflowProfile]:
    entries = load_training_workflow_entries(path)
    return build_sample_type_profiles(entries)


def match_profile_for_barcode(
    profiles: Sequence[SampleTypeWorkflowProfile],
    barcode: str,
) -> Optional[SampleTypeWorkflowProfile]:
    b = _norm_token(barcode)
    if not b:
        return None

    exact_candidates: List[Tuple[int, SampleTypeWorkflowProfile]] = []
    contains_candidates: List[Tuple[int, SampleTypeWorkflowProfile]] = []

    for profile in profiles:
        for material in profile.material_codes:
            token = _norm_token(material)
            if not _is_specific_id_token(token):
                continue
            if _endswith_token_boundary(b, token):
                exact_candidates.append((len(token), profile))
            elif _contains_token_boundary(b, token):
                contains_candidates.append((len(token), profile))
        for article in profile.article_numbers:
            token = _norm_token(article)
            if not _is_specific_id_token(token):
                continue
            if _contains_token_boundary(b, token):
                contains_candidates.append((len(token), profile))

    if exact_candidates:
        exact_candidates.sort(key=lambda x: x[0], reverse=True)
        return exact_candidates[0][1]
    if contains_candidates:
        contains_candidates.sort(key=lambda x: x[0], reverse=True)
        return contains_candidates[0][1]
    return None


def match_entry_for_barcode(
    entries: Sequence[TrainingWorkflowEntry],
    barcode: str,
) -> Optional[TrainingWorkflowEntry]:
    b = _normalize_text(barcode).upper()
    if not b:
        return None

    exact_candidates: List[Tuple[int, TrainingWorkflowEntry]] = []
    contains_candidates: List[Tuple[int, TrainingWorkflowEntry]] = []

    for entry in entries:
        material = _normalize_text(entry.material_code)
        article = _normalize_text(entry.article_number).replace(" ", "")
        if material:
            mat_upper = material.upper()
            if _is_specific_id_token(mat_upper) and _endswith_token_boundary(b, _norm_token(mat_upper)):
                exact_candidates.append((len(mat_upper), entry))
            elif _is_specific_id_token(mat_upper) and _contains_token_boundary(b, _norm_token(mat_upper)):
                contains_candidates.append((len(mat_upper), entry))
        if article:
            art_upper = article.upper()
            if _is_specific_id_token(art_upper) and _contains_token_boundary(b, _norm_token(art_upper)):
                contains_candidates.append((len(art_upper), entry))

    if exact_candidates:
        exact_candidates.sort(key=lambda x: x[0], reverse=True)
        return exact_candidates[0][1]
    if contains_candidates:
        contains_candidates.sort(key=lambda x: x[0], reverse=True)
        return contains_candidates[0][1]
    return None
