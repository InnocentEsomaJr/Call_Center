from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Dashboard Call Center", layout="wide", initial_sidebar_state="expanded")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PICTURES_CALL_CENTER_DIR = Path.home() / "Pictures" / "Call Center"
DEFAULT_APPELS_FILENAME = "APPELS COUSP DU 24 FEVRIER 2026.xlsx"
DEFAULT_ALERTES_FILENAME = "ALERTE COUSP DU 25 FEVRIER 2026.xlsx"

THEME = {
    "teal": "#005f73",
    "teal_dark": "#003f4b",
    "blue": "#1d9bf0",
    "pink": "#ef4b9b",
    "green": "#1f9d55",
    "red": "#dc2626",
    "slate": "#4b5563",
    "light": "#f7fbfc",
}

PROVINCE_COORDS = {
    "Kinshasa": (-4.325, 15.322),
    "Kongo Central": (-5.252, 14.865),
    "Kwango": (-6.200, 17.483),
    "Kwilu": (-5.040, 18.817),
    "Mai-Ndombe": (-2.250, 18.300),
    "Kasai": (-6.000, 21.500),
    "Kasai Central": (-6.150, 23.600),
    "Kasai Oriental": (-6.120, 23.590),
    "Lomami": (-6.140, 24.480),
    "Sankuru": (-2.850, 23.430),
    "Maniema": (-2.950, 26.200),
    "Sud Kivu": (-3.000, 28.100),
    "Nord Kivu": (-0.780, 29.250),
    "Ituri": (1.550, 30.250),
    "Haut-Uele": (3.250, 27.600),
    "Bas-Uele": (3.650, 24.950),
    "Tshopo": (0.520, 25.200),
    "Mongala": (2.300, 21.300),
    "Equateur": (0.100, 19.800),
    "Sud-Ubangi": (3.300, 19.000),
    "Nord-Ubangi": (4.000, 22.000),
    "Tshuapa": (-0.100, 22.700),
    "Tanganyika": (-6.500, 27.450),
    "Haut-Lomami": (-7.800, 24.500),
    "Lualaba": (-10.700, 25.300),
    "Haut-Katanga": (-11.670, 27.480),
}

PROVINCE_ALIASES = {
    "kinshasa": "Kinshasa",
    "kongo central": "Kongo Central",
    "kwango": "Kwango",
    "kwilu": "Kwilu",
    "mai ndombe": "Mai-Ndombe",
    "mai-ndombe": "Mai-Ndombe",
    "kasai": "Kasai",
    "kasai central": "Kasai Central",
    "kasai oriental": "Kasai Oriental",
    "lomami": "Lomami",
    "sankuru": "Sankuru",
    "maniema": "Maniema",
    "sud kivu": "Sud Kivu",
    "nord kivu": "Nord Kivu",
    "ituri": "Ituri",
    "haut uele": "Haut-Uele",
    "bas uele": "Bas-Uele",
    "tshopo": "Tshopo",
    "mongala": "Mongala",
    "equateur": "Equateur",
    "sud ubangi": "Sud-Ubangi",
    "nord ubangi": "Nord-Ubangi",
    "tshuapa": "Tshuapa",
    "tanganyika": "Tanganyika",
    "haut lomami": "Haut-Lomami",
    "lualaba": "Lualaba",
    "haut katanga": "Haut-Katanga",
}

MONTH_TOKEN_MAP = {
    "janvier": 1,
    "janv": 1,
    "january": 1,
    "fevrier": 2,
    "fevr": 2,
    "fev": 2,
    "february": 2,
    "mars": 3,
    "march": 3,
    "avril": 4,
    "avr": 4,
    "april": 4,
    "mai": 5,
    "may": 5,
    "juin": 6,
    "june": 6,
    "juillet": 7,
    "juil": 7,
    "july": 7,
    "aout": 8,
    "august": 8,
    "septembre": 9,
    "sept": 9,
    "september": 9,
    "octobre": 10,
    "oct": 10,
    "october": 10,
    "novembre": 11,
    "nov": 11,
    "november": 11,
    "decembre": 12,
    "dec": 12,
    "december": 12,
}

CALL_COLUMN_ALIASES = {
    "date": ["date", "periode", "period", "jour", "timestamp"],
    "heure": ["heure", "hour"],
    "province": ["province", "prov"],
    "territoire": ["territoire", "territory", "zone", "district", "ville", "commune"],
    "details": ["details", "detail", "description", "nature", "message", "motif", "sujet", "appel"],
    "incident": ["incident", "pathologie", "maladie", "patho", "type"],
    "categorie": ["categorie", "category", "type demande", "type appel", "classification"],
    "genre": ["genre", "sexe", "gender", "sex"],
    "statut": ["statut", "status", "resolution", "resolu", "conclusion", "etat"],
    "record_count": ["record count", "count", "nombre", "nb", "qty", "quantite", "valeur", "value"],
}

ALERT_COLUMN_ALIASES = {
    "date": ["date", "periode", "period", "mois", "month", "semaine"],
    "heure": ["heure", "hour"],
    "location": ["province", "organisation unit", "organisationunit", "localite", "location", "zone", "territoire"],
    "indicator": ["indicateur", "indicator", "data", "type", "nature", "categorie"],
    "value": ["value", "valeur", "record count", "count", "nombre", "nb", "qty"],
    "details": ["details", "detail", "description", "commentaire", "message"],
}


@dataclass
class DataSourceInfo:
    source: str
    sheet_name: str
    note: str


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


def canonical_province(value: object) -> str:
    raw = "" if pd.isna(value) else str(value).strip()
    if not raw:
        return "Inconnu"
    norm = normalize_text(raw)
    if norm in PROVINCE_ALIASES:
        return PROVINCE_ALIASES[norm]
    for alias, canonical in PROVINCE_ALIASES.items():
        if alias in norm:
            return canonical
    return raw


def normalize_gender(value: object) -> str:
    norm = normalize_text(value)
    if norm in {"h", "homme", "masculin", "male", "m"}:
        return "Homme"
    if norm in {"f", "femme", "feminin", "female"}:
        return "Femme"
    return "ND"


def normalize_status(value: object) -> str:
    norm = normalize_text(value)
    if any(token in norm for token in ["resolu", "resolved", "traite", "cloture", "ferme", "close"]):
        return "Resolu"
    if any(token in norm for token in ["non", "ouvert", "pending", "attente", "cours"]):
        return "Non resolu"
    return "Non resolu"


def clean_label(value: object, fallback: str) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return text if text else fallback


def make_column_map(df: pd.DataFrame, aliases: dict[str, list[str]]) -> dict[str, str]:
    normalized_cols = {col: normalize_text(col) for col in df.columns}
    selected: dict[str, str] = {}
    used: set[str] = set()

    for target, patterns in aliases.items():
        chosen = None
        for col, norm_col in normalized_cols.items():
            if col in used:
                continue
            if any(pattern in norm_col for pattern in patterns):
                chosen = col
                break
        if chosen:
            selected[target] = chosen
            used.add(chosen)
    return selected


def parse_month_from_label(label: object) -> tuple[pd.Timestamp | None, str]:
    normalized = normalize_text(label)
    if not normalized:
        return None, ""

    year_match = re.search(r"(20\d{2})", normalized)
    if not year_match:
        return None, ""
    year = int(year_match.group(1))

    month_value = None
    month_token = ""
    for token, month_num in MONTH_TOKEN_MAP.items():
        if re.search(rf"\b{re.escape(token)}\b", normalized):
            month_value = month_num
            month_token = token
            break

    if month_value is None:
        return None, ""

    prefix = normalized.split(str(year))[0]
    if month_token:
        prefix = re.sub(rf"\b{re.escape(month_token)}\b", "", prefix).strip()
    prefix = re.sub(r"\s+", " ", prefix).strip(" -_/:")

    try:
        date_value = pd.Timestamp(year=year, month=month_value, day=1)
    except ValueError:
        return None, ""

    return date_value, prefix


def parse_excel_date_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    excel_dates = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")

    numeric_mask = numeric.notna()
    invalid_or_suspect = parsed.isna() | (parsed < pd.Timestamp("1990-01-01")) | (parsed > pd.Timestamp("2100-12-31"))
    replace_mask = numeric_mask & invalid_or_suspect
    parsed.loc[replace_mask] = excel_dates.loc[replace_mask]
    return parsed


def parse_time_delta_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    delta = pd.Series(pd.NaT, index=series.index, dtype="timedelta64[ns]")

    day_fraction_mask = numeric.notna() & numeric.between(0, 1, inclusive="both")
    if day_fraction_mask.any():
        seconds = (numeric.loc[day_fraction_mask] * 86400).round()
        delta.loc[day_fraction_mask] = pd.to_timedelta(seconds, unit="s")

    hour_mask = numeric.notna() & numeric.between(1, 24, inclusive="left")
    if hour_mask.any():
        seconds = (numeric.loc[hour_mask] * 3600).round()
        delta.loc[hour_mask] = pd.to_timedelta(seconds, unit="s")

    text_delta = pd.to_timedelta(series.astype(str), errors="coerce")
    delta = delta.fillna(text_delta)
    return delta


def parse_datetime_columns(df: pd.DataFrame, date_col: str | None, hour_col: str | None) -> pd.Series:
    if date_col is None or date_col not in df.columns:
        base = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    else:
        base = parse_excel_date_series(df[date_col])

    if hour_col and hour_col in df.columns:
        delta = parse_time_delta_series(df[hour_col]).fillna(pd.Timedelta(0))
        base = base + delta

    return pd.to_datetime(base, errors="coerce")


def _read_best_sheet(source: object) -> tuple[pd.DataFrame, str]:
    xls = pd.ExcelFile(source)
    best_sheet = xls.sheet_names[0]
    best_df = pd.DataFrame()
    best_score = -1.0

    for sheet in xls.sheet_names:
        frame = pd.read_excel(xls, sheet_name=sheet)
        if frame.empty:
            continue

        valid_cols = [c for c in frame.columns if "unnamed" not in normalize_text(c)]
        if not valid_cols:
            valid_cols = list(frame.columns)

        non_empty = 0.0
        if valid_cols:
            non_empty = float(frame[valid_cols].notna().mean().mean())

        score = len(valid_cols) * 1000 + min(len(frame), 5000) + (non_empty * 100)
        if score > best_score:
            best_score = score
            best_sheet = sheet
            best_df = frame

    if best_df.empty:
        best_df = pd.read_excel(xls, sheet_name=best_sheet)

    return best_df, best_sheet


@st.cache_data(show_spinner=False)
def read_excel_best_sheet_from_path(path_str: str) -> tuple[pd.DataFrame, str]:
    return _read_best_sheet(path_str)


@st.cache_data(show_spinner=False)
def read_excel_best_sheet_from_bytes(binary: bytes) -> tuple[pd.DataFrame, str]:
    return _read_best_sheet(BytesIO(binary))


def find_local_excel(kind: str) -> Path | None:
    search_roots = [DATA_DIR, BASE_DIR, BASE_DIR.parent / "MesAnalyses", PICTURES_CALL_CENTER_DIR]
    exact_candidates = {
        "appels": [
            DEFAULT_APPELS_FILENAME,
            "appels.xlsx",
            "appels.xls",
            "appel.xlsx",
            "appel.xls",
        ],
        "alertes": [
            DEFAULT_ALERTES_FILENAME,
            "alertes.xlsx",
            "alertes.xls",
            "alerte.xlsx",
            "alerte.xls",
        ],
    }
    wildcard_candidates = {
        "appels": ["*appels*.xlsx", "*appels*.xls", "*appel*.xlsx", "*appel*.xls"],
        "alertes": ["*alertes*.xlsx", "*alertes*.xls", "*alerte*.xlsx", "*alerte*.xls"],
    }

    for root in search_roots:
        if not root.exists():
            continue
        for filename in exact_candidates[kind]:
            candidate = root / filename
            if candidate.exists() and candidate.is_file():
                return candidate

    ranked: list[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        for pattern in wildcard_candidates[kind]:
            ranked.extend([p for p in root.glob(pattern) if p.is_file()])

    if not ranked:
        return None

    ranked = sorted(set(ranked), key=lambda p: p.stat().st_mtime, reverse=True)
    return ranked[0]


def build_demo_calls(rows: int = 14000) -> pd.DataFrame:
    rng = np.random.default_rng(7)

    provinces = list(PROVINCE_COORDS.keys())
    incidents = [
        "Signes & Symptomes",
        "Mpox",
        "Generalites",
        "Paludisme",
        "Cholera",
        "Ebola",
        "VIH/SIDA",
        "Sante animale",
        "Tuberculose",
        "Rougeole",
        "Typhoide",
        "Covid-19",
    ]
    categories = [
        "Suggestions /Demandes/ Requetes",
        "Questions/Preoccupations",
        "Alerte",
        "Plaintes/Denonciations/Reclamations",
        "Reconnaissance/Remerciement",
    ]
    detail_templates = [
        "L'appelant voulait avoir le remede contre les maux de tete.",
        "L'appelant voulait avoir le remede contre les maux de ventre.",
        "L'appelant demande des medicaments pour soigner les maux de ventre.",
        "L'appelant dit etre malade.",
        "Signalement d'un cas suspect dans la communaute.",
    ]

    start = np.datetime64("2025-01-01")
    end = np.datetime64("2026-02-15")
    date_values = start + rng.integers(0, (end - start).astype(int), size=rows).astype("timedelta64[D]")

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(date_values),
            "province": rng.choice(provinces, size=rows),
            "territoire": rng.choice(
                ["Kinshasa", "Lukolela", "Inongo", "Bandalungwa", "Mont Ngafula", "Kabinda", "Goma", "Bukavu"],
                size=rows,
            ),
            "details": rng.choice(detail_templates, size=rows),
            "incident": rng.choice(
                incidents,
                size=rows,
                p=[0.52, 0.19, 0.09, 0.05, 0.025, 0.015, 0.013, 0.01, 0.009, 0.008, 0.006, 0.004],
            ),
            "categorie": rng.choice(categories, size=rows, p=[0.51, 0.38, 0.08, 0.02, 0.01]),
            "genre": rng.choice(["Homme", "Femme", "ND"], size=rows, p=[0.957, 0.042, 0.001]),
            "statut": rng.choice(["Resolu", "Non resolu"], size=rows, p=[0.86, 0.14]),
            "record_count": 1,
        }
    )

    return frame


def standardize_calls(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "province",
                "territoire",
                "details",
                "incident",
                "categorie",
                "genre",
                "statut",
                "record_count",
            ]
        )

    mapping = make_column_map(raw, CALL_COLUMN_ALIASES)
    out = pd.DataFrame(index=raw.index)

    out["date"] = parse_datetime_columns(raw, mapping.get("date"), mapping.get("heure"))

    out["province"] = raw[mapping["province"]] if "province" in mapping else "Inconnu"
    out["territoire"] = raw[mapping["territoire"]] if "territoire" in mapping else out["province"]

    if "details" in mapping:
        out["details"] = raw[mapping["details"]]
    elif "incident" in mapping:
        out["details"] = raw[mapping["incident"]]
    else:
        out["details"] = "Sans detail"

    out["incident"] = raw[mapping["incident"]] if "incident" in mapping else "Non precise"
    out["categorie"] = raw[mapping["categorie"]] if "categorie" in mapping else "Non classe"
    out["genre"] = raw[mapping["genre"]] if "genre" in mapping else "ND"
    out["statut"] = raw[mapping["statut"]] if "statut" in mapping else "Non resolu"

    if "record_count" in mapping:
        out["record_count"] = pd.to_numeric(raw[mapping["record_count"]], errors="coerce").fillna(1)
    else:
        out["record_count"] = 1

    if out["date"].isna().all():
        out["date"] = pd.Timestamp.today().normalize()
    else:
        out["date"] = out["date"].fillna(out["date"].dropna().min())

    out["province"] = out["province"].map(canonical_province)
    out["territoire"] = out["territoire"].map(lambda x: clean_label(x, "Inconnu"))
    out["details"] = out["details"].map(lambda x: clean_label(x, "Sans detail"))
    out["incident"] = out["incident"].map(lambda x: clean_label(x, "Non precise"))
    out["categorie"] = out["categorie"].map(lambda x: clean_label(x, "Non classe"))
    out["genre"] = out["genre"].map(normalize_gender)
    out["statut"] = out["statut"].map(normalize_status)
    out["record_count"] = pd.to_numeric(out["record_count"], errors="coerce").fillna(1).clip(lower=0)

    return out


def standardize_alerts(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "location", "indicator", "value", "details"])

    mapping = make_column_map(raw, ALERT_COLUMN_ALIASES)
    month_columns: list[tuple[str, pd.Timestamp, str]] = []

    for col in raw.columns:
        month_dt, prefix = parse_month_from_label(col)
        if month_dt is not None:
            month_columns.append((col, month_dt, prefix))

    if month_columns:
        value_cols = [col for col, _, _ in month_columns]
        id_vars = [mapping[col] for col in ["location", "indicator", "details"] if col in mapping]

        melted = raw.melt(id_vars=id_vars, value_vars=value_cols, var_name="raw_period", value_name="value")
        melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
        melted = melted.dropna(subset=["value"])

        period_map = {col: dt for col, dt, _ in month_columns}
        prefix_map = {col: pref for col, _, pref in month_columns}

        melted["date"] = melted["raw_period"].map(period_map)
        if "indicator" in mapping:
            melted["indicator"] = melted[mapping["indicator"]]
        else:
            melted["indicator"] = melted["raw_period"].map(prefix_map)

        if "location" in mapping:
            melted["location"] = melted[mapping["location"]]
        else:
            melted["location"] = "Inconnu"

        if "details" in mapping:
            melted["details"] = melted[mapping["details"]]
        else:
            melted["details"] = ""

        out = melted[["date", "location", "indicator", "value", "details"]].copy()
    else:
        out = pd.DataFrame(index=raw.index)
        out["date"] = parse_datetime_columns(raw, mapping.get("date"), mapping.get("heure"))
        out["location"] = raw[mapping["location"]] if "location" in mapping else "Inconnu"
        out["indicator"] = raw[mapping["indicator"]] if "indicator" in mapping else "Alerte"
        if "value" in mapping:
            out["value"] = pd.to_numeric(raw[mapping["value"]], errors="coerce").fillna(1)
        else:
            out["value"] = 1
        out["details"] = raw[mapping["details"]] if "details" in mapping else ""

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if out["date"].isna().all():
        out["date"] = pd.Timestamp.today().normalize()
    else:
        out["date"] = out["date"].fillna(out["date"].dropna().min())

    out["location"] = out["location"].map(lambda x: clean_label(x, "Inconnu"))
    out["indicator"] = out["indicator"].map(lambda x: clean_label(x, "Alerte"))
    out["details"] = out["details"].map(lambda x: clean_label(x, ""))
    out["value"] = pd.to_numeric(out["value"], errors="coerce").fillna(0).clip(lower=0)

    return out


def load_calls_data(uploaded_files: list[object] | None) -> tuple[pd.DataFrame, DataSourceInfo]:
    files = [f for f in (uploaded_files or []) if f is not None]
    if not files:
        empty = pd.DataFrame(
            columns=[
                "date",
                "province",
                "territoire",
                "details",
                "incident",
                "categorie",
                "genre",
                "statut",
                "record_count",
            ]
        )
        return empty, DataSourceInfo("Aucune source", "-", "Veuillez importer au moins un fichier APPELS.")

    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []
    for file_obj in files:
        try:
            frame, sheet = read_excel_best_sheet_from_bytes(file_obj.getvalue())
            frames.append(standardize_calls(frame))
            loaded_files.append(f"{file_obj.name} ({sheet})")
        except Exception:
            loaded_files.append(f"{file_obj.name} (erreur)")

    if frames:
        merged = pd.concat(frames, ignore_index=True)
    else:
        merged = pd.DataFrame(columns=["date", "province", "territoire", "details", "incident", "categorie", "genre", "statut", "record_count"])

    note = f"{len(files)} fichier(s) APPELS importé(s)."
    if loaded_files:
        note += " " + " | ".join(loaded_files[:5])
        if len(loaded_files) > 5:
            note += f" | +{len(loaded_files) - 5} autres"
    return merged, DataSourceInfo("Upload multiple", "-", note)


def load_alerts_data(uploaded_files: list[object] | None) -> tuple[pd.DataFrame, DataSourceInfo]:
    files = [f for f in (uploaded_files or []) if f is not None]
    if not files:
        empty = pd.DataFrame(columns=["date", "location", "indicator", "value", "details"])
        return empty, DataSourceInfo("Aucune source", "-", "Veuillez importer au moins un fichier ALERTES.")

    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []
    for file_obj in files:
        try:
            frame, sheet = read_excel_best_sheet_from_bytes(file_obj.getvalue())
            frames.append(standardize_alerts(frame))
            loaded_files.append(f"{file_obj.name} ({sheet})")
        except Exception:
            loaded_files.append(f"{file_obj.name} (erreur)")

    if frames:
        merged = pd.concat(frames, ignore_index=True)
    else:
        merged = pd.DataFrame(columns=["date", "location", "indicator", "value", "details"])

    note = f"{len(files)} fichier(s) ALERTES importé(s)."
    if loaded_files:
        note += " " + " | ".join(loaded_files[:5])
        if len(loaded_files) > 5:
            note += f" | +{len(loaded_files) - 5} autres"
    return merged, DataSourceInfo("Upload multiple", "-", note)


def format_int(value: float) -> str:
    return f"{int(round(value)):,}".replace(",", " ")


def add_bar_value_labels(fig, orientation: str = "v", is_percent: bool = False) -> None:
    if orientation == "h":
        template = "%{x:.1f}%" if is_percent else "%{x:,.0f}"
    else:
        template = "%{y:.1f}%" if is_percent else "%{y:,.0f}"
    fig.update_traces(texttemplate=template, textposition="outside", cliponaxis=False)
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")


def add_line_value_labels(fig, is_percent: bool = False) -> None:
    template = "%{y:.1f}%" if is_percent else "%{y:,.0f}"
    fig.update_traces(mode="lines+markers+text", texttemplate=template, textposition="top center")
    fig.update_layout(uniformtext_minsize=7, uniformtext_mode="hide")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;600;700&family=Rajdhani:wght@600;700&display=swap');

        .stApp {
            background: radial-gradient(circle at top left, #0b121a 0%, #0f1724 55%, #111827 100%);
            font-family: 'Barlow', sans-serif;
            color: #e7eef6;
        }
        .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6, .stApp p, .stApp label {
            color: #e7eef6;
        }
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
        }
        .cc-header {
            display: flex;
            align-items: center;
            gap: 12px;
            background: linear-gradient(90deg, #005f73 0%, #00758d 60%, #005f73 100%);
            color: #ffffff;
            border: 2px solid #0d2f38;
            border-radius: 8px;
            padding: 0.75rem 1rem;
            margin-bottom: 0.75rem;
            animation: ccFadeIn 450ms ease-out;
        }
        .cc-header .logo {
            background: #ffffff;
            color: #003f4b;
            border-radius: 6px;
            font-weight: 700;
            padding: 6px 10px;
            font-size: 0.86rem;
        }
        .cc-header .title {
            font-family: 'Rajdhani', sans-serif;
            letter-spacing: 0.02em;
            font-size: 1.55rem;
            font-weight: 700;
        }
        .kpi-wrap {
            background: transparent;
            padding: 0.25rem 0;
            animation: ccRise 420ms ease-out;
        }
        .kpi-pill {
            height: 34px;
            background: #e9eef1;
            border: 1px solid #afbec7;
            border-radius: 14px;
            margin-bottom: 0.6rem;
        }
        .kpi-card {
            border-radius: 14px;
            border: 3px solid #0d3b52;
            color: #ffffff;
            padding: 0.45rem 0.55rem 0.55rem 0.55rem;
            text-align: center;
            margin-bottom: 0.65rem;
            box-shadow: 0 2px 0 rgba(9, 40, 56, 0.4);
        }
        .kpi-label {
            font-size: 1.02rem;
            font-weight: 700;
            line-height: 1.1;
        }
        .kpi-value {
            font-family: 'Rajdhani', sans-serif;
            font-size: 2.2rem;
            font-weight: 700;
            line-height: 1;
            margin-top: 0.25rem;
        }
        .kpi-horizontal-wrap {
            margin: 0.2rem 0 0.7rem 0;
            padding: 0.55rem 0.55rem 0.25rem 0.55rem;
            border: 1px solid #1f3f52;
            background: #0f2230;
            border-radius: 12px;
        }
        .kpi-horizontal-grid {
            display: grid;
            grid-template-columns: repeat(7, minmax(130px, 1fr));
            gap: 0.45rem;
            overflow-x: auto;
            padding-bottom: 0.2rem;
        }
        .kpi-horizontal-card {
            border-radius: 14px;
            border: 2px solid #103a4f;
            color: #ffffff;
            text-align: center;
            padding: 0.25rem 0.45rem 0.35rem 0.45rem;
            min-height: 72px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            box-shadow: 0 2px 0 rgba(9, 40, 56, 0.35);
        }
        .kpi-horizontal-label {
            font-size: 0.95rem;
            font-weight: 700;
            line-height: 1.05;
        }
        .kpi-horizontal-value {
            font-family: 'Rajdhani', sans-serif;
            font-size: 2rem;
            font-weight: 700;
            line-height: 1;
            margin-top: 0.18rem;
        }
        .source-note {
            border-left: 4px solid #005f73;
            background: #13202c;
            padding: 0.45rem 0.65rem;
            margin: 0.3rem 0;
            border-radius: 6px;
            font-size: 0.88rem;
            color: #d8e6f5;
        }
        .upload-banner {
            border: 1px dashed #0d677b;
            background: #0f2230;
            border-radius: 8px;
            padding: 0.75rem 0.9rem;
            margin: 0.35rem 0 0.65rem 0;
            color: #d8e6f5;
            font-size: 0.92rem;
        }
        .filter-title {
            background: #0f2d3a;
            border: 1px solid #1f4a5c;
            border-radius: 8px;
            color: #e8f5ff;
            padding: 0.45rem 0.65rem;
            margin-bottom: 0.55rem;
            font-weight: 700;
            font-size: 0.96rem;
        }
        @keyframes ccFadeIn {
            from { opacity: 0; transform: translateY(-8px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes ccRise {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    st.markdown(
        """
        <div class="cc-header">
            <div class="logo">CALL CENTER</div>
            <div class="title">DASHBOARD CALL CENTER</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_source_notes(calls_info: DataSourceInfo, alerts_info: DataSourceInfo) -> None:
    st.markdown(
        f"<div class='source-note'><b>Appels</b>: {calls_info.note}<br><b>Source</b>: {calls_info.source} | <b>Feuille</b>: {calls_info.sheet_name}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='source-note'><b>Alertes</b>: {alerts_info.note}<br><b>Source</b>: {alerts_info.source} | <b>Feuille</b>: {alerts_info.sheet_name}</div>",
        unsafe_allow_html=True,
    )


def render_kpi_card(label: str, value: float, color: str) -> str:
    return (
        f"<div class='kpi-card' style='background:{color};'>"
        f"<div class='kpi-label'>{label}</div>"
        f"<div class='kpi-value'>{format_int(value)}</div>"
        f"</div>"
    )


def compute_kpis(filtered: pd.DataFrame) -> dict[str, float]:
    total_calls = filtered["record_count"].sum()
    provinces_count = filtered["province"].nunique()
    resolved_calls = filtered.loc[filtered["statut"] == "Resolu", "record_count"].sum()
    unresolved_calls = max(total_calls - resolved_calls, 0)
    male_calls = filtered.loc[filtered["genre"] == "Homme", "record_count"].sum()
    female_calls = filtered.loc[filtered["genre"] == "Femme", "record_count"].sum()
    nd_calls = filtered.loc[filtered["genre"] == "ND", "record_count"].sum()
    return {
        "provinces_count": provinces_count,
        "total_calls": total_calls,
        "resolved_calls": resolved_calls,
        "unresolved_calls": unresolved_calls,
        "male_calls": male_calls,
        "female_calls": female_calls,
        "nd_calls": nd_calls,
    }


def render_kpi_panel(kpis: dict[str, float]) -> None:
    st.markdown("<div class='kpi-wrap'>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-pill'></div>", unsafe_allow_html=True)
    st.markdown(render_kpi_card("Province", kpis["provinces_count"], "#1d95ab"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("Tot appels", kpis["total_calls"], "#2d80c1"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("Resolus", kpis["resolved_calls"], "#23a153"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("Non resolus", kpis["unresolved_calls"], "#e61f25"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("Hommes", kpis["male_calls"], "#2f95da"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("Femmes", kpis["female_calls"], "#e04a98"), unsafe_allow_html=True)
    st.markdown(render_kpi_card("ND", kpis["nd_calls"], "#909db0"), unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_kpi_horizontal(kpis: dict[str, float]) -> None:
    cards = [
        ("Province", kpis["provinces_count"], "#1d95ab"),
        ("Tot appels", kpis["total_calls"], "#2d80c1"),
        ("Resolu", kpis["resolved_calls"], "#23a153"),
        ("Non resolu", kpis["unresolved_calls"], "#e61f25"),
        ("Hommes", kpis["male_calls"], "#2f95da"),
        ("Femmes", kpis["female_calls"], "#e04a98"),
        ("ND", kpis["nd_calls"], "#909db0"),
    ]
    html_parts = ["<div class='kpi-horizontal-wrap'><div class='kpi-horizontal-grid'>"]
    for label, value, color in cards:
        html_parts.append(
            f"<div class='kpi-horizontal-card' style='background:{color};'>"
            f"<div class='kpi-horizontal-label'>{label}</div>"
            f"<div class='kpi-horizontal-value'>{format_int(value)}</div>"
            "</div>"
        )
    html_parts.append("</div></div>")
    st.markdown("".join(html_parts), unsafe_allow_html=True)


def group_by_day(df: pd.DataFrame, value_col: str, category_col: str | None = None) -> pd.DataFrame:
    frame = df.copy()
    frame["__day"] = pd.to_datetime(frame["date"], errors="coerce").dt.floor("D")
    frame = frame.dropna(subset=["__day"])
    if frame.empty:
        columns = ["date", value_col] if category_col is None else ["date", category_col, value_col]
        return pd.DataFrame(columns=columns)

    group_cols = ["__day"] + ([category_col] if category_col else [])
    out = (
        frame.groupby(group_cols, as_index=False)[value_col]
        .sum()
        .rename(columns={"__day": "date"})
        .sort_values("date")
    )
    return out


def apply_calls_filters(
    calls_df: pd.DataFrame,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    provinces: list[str],
    genres: list[str],
    incidents: list[str],
    categories: list[str],
) -> pd.DataFrame:
    mask = (calls_df["date"] >= date_start) & (calls_df["date"] <= date_end)

    if provinces:
        mask &= calls_df["province"].isin(provinces)
    if genres:
        mask &= calls_df["genre"].isin(genres)
    if incidents:
        mask &= calls_df["incident"].isin(incidents)
    if categories:
        mask &= calls_df["categorie"].isin(categories)

    return calls_df.loc[mask].copy()


def apply_alerts_filters(
    alerts_df: pd.DataFrame,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    provinces: list[str],
) -> pd.DataFrame:
    if alerts_df.empty:
        return alerts_df.copy()

    mask = (alerts_df["date"] >= date_start) & (alerts_df["date"] <= date_end)
    out = alerts_df.loc[mask].copy()
    if provinces:
        wanted = {canonical_province(p) for p in provinces}
        loc_canon = out["location"].map(canonical_province)
        out = out.loc[loc_canon.isin(wanted)].copy()
    return out


def compute_completeness_table(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    fields = [
        ("date", "Date"),
        ("province", "Province"),
        ("territoire", "Territoire"),
        ("genre", "Genre"),
        ("categorie", "Categorie d'appel"),
        ("incident", "Incident/Pathologie"),
        ("details", "Details de l'appel"),
        ("statut", "Statut"),
    ]
    missing_tokens = {"", "nan", "none", "inconnu", "nd", "non precise", "non classe", "sans detail"}
    rows = []

    for col, label in fields:
        if col not in df.columns:
            continue
        series = df[col]
        valid = series.notna()
        string_series = series.astype(str).str.strip().str.lower()
        valid &= ~string_series.isin(missing_tokens)
        total = int(len(series))
        filled = int(valid.sum())
        rate = (filled / total * 100.0) if total else 0.0
        rows.append({"Champ": label, "Renseigne": filled, "Total": total, "Completude (%)": round(rate, 1)})

    if not rows:
        return pd.DataFrame(columns=["Champ", "Renseigne", "Total", "Completude (%)"]), 0.0

    table = pd.DataFrame(rows)
    overall = float(table["Renseigne"].sum()) / float(table["Total"].sum()) * 100.0 if table["Total"].sum() else 0.0
    return table, round(overall, 1)


def render_interactive_analytics(
    filtered_calls: pd.DataFrame,
    previous_calls: pd.DataFrame,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
) -> None:
    st.markdown("<div class='filter-title'>Analyses avancees interactives</div>", unsafe_allow_html=True)
    options = st.multiselect(
        "Choisissez les analyses a afficher",
        options=[
            "Qualite des donnees (completude)",
            "Performance de resolution",
            "Comparaison periode precedente",
            "Profil des appels",
        ],
        default=[
            "Qualite des donnees (completude)",
            "Performance de resolution",
            "Comparaison periode precedente",
        ],
        key="analysis_options",
    )

    if filtered_calls.empty:
        st.info("Aucune donnee disponible pour calculer les analyses avancees.")
        return

    total_calls = float(filtered_calls["record_count"].sum())
    resolved = float(filtered_calls.loc[filtered_calls["statut"] == "Resolu", "record_count"].sum())
    resolution_rate = (resolved / total_calls * 100.0) if total_calls > 0 else 0.0
    alert_volume = float(
        filtered_calls.loc[filtered_calls["categorie"].astype(str).str.contains("alerte", case=False, na=False), "record_count"].sum()
    )
    alert_rate = (alert_volume / total_calls * 100.0) if total_calls > 0 else 0.0

    if "Performance de resolution" in options:
        c1, c2, c3 = st.columns(3)
        c1.metric("Taux de resolution", f"{resolution_rate:.1f}%")
        c2.metric("Volume alerte (appels)", format_int(alert_volume))
        c3.metric("Part alerte", f"{alert_rate:.1f}%")

    if "Comparaison periode precedente" in options:
        prev_total = float(previous_calls["record_count"].sum()) if not previous_calls.empty else 0.0
        delta_pct = ((total_calls - prev_total) / prev_total * 100.0) if prev_total > 0 else np.nan
        window_days = (date_end.normalize() - date_start.normalize()).days + 1
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Periode active", f"{window_days} jours")
        col_b.metric("Appels (periode active)", format_int(total_calls))
        if np.isnan(delta_pct):
            col_c.metric("Variation vs periode precedente", "N/A")
        else:
            col_c.metric("Variation vs periode precedente", f"{delta_pct:+.1f}%")

    if "Qualite des donnees (completude)" in options:
        comp_table, overall = compute_completeness_table(filtered_calls)
        c1, c2 = st.columns([1.1, 2.2], gap="small")
        with c1:
            st.metric("Completuude globale", f"{overall:.1f}%")
            weak = comp_table[comp_table["Completude (%)"] < 90.0]
            st.metric("Champs < 90%", int(len(weak)))
        with c2:
            fig_comp = px.bar(
                comp_table.sort_values("Completude (%)", ascending=True),
                x="Completude (%)",
                y="Champ",
                orientation="h",
                range_x=[0, 100],
                color="Completude (%)",
                color_continuous_scale="RdYlGn",
            )
            add_bar_value_labels(fig_comp, orientation="h", is_percent=True)
            fig_comp.update_layout(height=280, margin=dict(l=0, r=0, t=5, b=0), coloraxis_showscale=False)
            st.plotly_chart(fig_comp, use_container_width=True)

    if "Profil des appels" in options:
        row = st.columns(2)
        with row[0]:
            top_incidents = (
                filtered_calls.groupby("incident", as_index=False)["record_count"]
                .sum()
                .sort_values("record_count", ascending=False)
                .head(8)
            )
            fig_inc = px.bar(top_incidents, x="incident", y="record_count", labels={"record_count": "Record Count", "incident": "Incident"})
            add_bar_value_labels(fig_inc, orientation="v")
            fig_inc.update_layout(height=280, margin=dict(l=0, r=0, t=5, b=0))
            st.plotly_chart(fig_inc, use_container_width=True)
        with row[1]:
            filtered_calls = filtered_calls.copy()
            filtered_calls["heure"] = pd.to_datetime(filtered_calls["date"], errors="coerce").dt.hour
            by_hour = filtered_calls.groupby("heure", as_index=False)["record_count"].sum().sort_values("heure")
            fig_hour = px.line(by_hour, x="heure", y="record_count", markers=True, labels={"heure": "Heure", "record_count": "Record Count"})
            add_line_value_labels(fig_hour)
            fig_hour.update_layout(height=280, margin=dict(l=0, r=0, t=5, b=0))
            st.plotly_chart(fig_hour, use_container_width=True)


def render_general_page(filtered: pd.DataFrame) -> None:
    if filtered.empty:
        st.warning("Aucune donnee d'appels pour ces filtres.")
        return

    kpis = compute_kpis(filtered)
    total_calls = kpis["total_calls"]
    provinces_count = kpis["provinces_count"]
    by_province = (
        filtered.groupby("province", as_index=False)["record_count"]
        .sum()
        .sort_values("record_count", ascending=True)
    )

    left_col, right_col = st.columns([2.7, 1.8], gap="small")

    with left_col:
        st.subheader("Nature/type d'appel")
        details = (
            filtered.groupby(["province", "territoire", "details"], as_index=False)["record_count"]
            .sum()
            .sort_values("record_count", ascending=False)
            .rename(columns={"province": "Province", "territoire": "Territoire", "details": "Details de l'appel", "record_count": "Record Count"})
        )
        details_preview = details.head(400)
        try:
            st.dataframe(
                details_preview.style.background_gradient(subset=["Record Count"], cmap="Reds"),
                use_container_width=True,
                height=470,
            )
        except Exception:
            # Streamlit Cloud may not have matplotlib; fallback to a plain dataframe.
            st.dataframe(
                details_preview,
                use_container_width=True,
                height=470,
            )

    with right_col:
        c1, c2 = st.columns(2)
        c1.metric("Nbre des provinces", format_int(provinces_count))
        c2.metric("Cumul des appels", format_int(total_calls))

        st.subheader("Proportion d'appel par province")
        fig_prov = px.bar(
            by_province,
            x="record_count",
            y="province",
            orientation="h",
            labels={"record_count": "Record Count", "province": "Province"},
            color="record_count",
            color_continuous_scale="Blues",
        )
        add_bar_value_labels(fig_prov, orientation="h")
        fig_prov.update_layout(margin=dict(l=10, r=10, t=5, b=10), coloraxis_showscale=False, height=265)
        st.plotly_chart(fig_prov, use_container_width=True)

    st.subheader("Geolocalisation des appels par province")
    map_rows = []
    for _, row in by_province.iterrows():
        province = row["province"]
        if province in PROVINCE_COORDS:
            lat, lon = PROVINCE_COORDS[province]
            map_rows.append({"province": province, "lat": lat, "lon": lon, "calls": row["record_count"]})

    if map_rows:
        map_df = pd.DataFrame(map_rows)
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat",
            lon="lon",
            size="calls",
            color="calls",
            hover_name="province",
            hover_data={"calls": True, "lat": False, "lon": False},
            color_continuous_scale="Tealgrn",
            size_max=48,
            zoom=4.55,
            center={"lat": -3.5, "lon": 23.6},
            mapbox_style="carto-darkmatter",
        )
        fig_map.update_traces(text=map_df["calls"].map(format_int), mode="markers+text", textposition="top center")
        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=560,
            coloraxis_showscale=False,
            mapbox=dict(pitch=0, bearing=0),
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Ajoutez des noms de province reconnus pour afficher la carte.")

    st.subheader("Evolution du nombre d'appels au fil du temps")
    trend = group_by_day(filtered, value_col="record_count", category_col="categorie")
    if trend.empty:
        st.info("Aucune donnee de tendance disponible.")
        return
    fig_trend = px.line(
        trend,
        x="date",
        y="record_count",
        color="categorie",
        labels={"record_count": "Record Count", "date": "Date", "categorie": "Categorie d'appel"},
    )
    add_line_value_labels(fig_trend)
    fig_trend.update_layout(legend_orientation="h", legend_y=1.06, margin=dict(l=0, r=0, t=20, b=0), height=330)
    fig_trend.update_traces(marker_size=4)
    st.plotly_chart(fig_trend, use_container_width=True)


def render_details_page(filtered: pd.DataFrame) -> None:
    if filtered.empty:
        st.warning("Aucune donnee d'appels pour ces filtres.")
        return

    top_row = st.columns(3, gap="small")

    with top_row[0]:
        st.subheader("Repartition par genre")
        by_gender = filtered.groupby("genre", as_index=False)["record_count"].sum()
        fig_gender = px.pie(
            by_gender,
            names="genre",
            values="record_count",
            hole=0.6,
            color="genre",
            color_discrete_map={"Homme": THEME["blue"], "Femme": THEME["pink"], "ND": "#94a3b8"},
        )
        fig_gender.update_traces(textinfo="label+percent+value")
        fig_gender.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=310)
        st.plotly_chart(fig_gender, use_container_width=True)

    with top_row[1]:
        st.subheader("Repartition par incident")
        by_incident = (
            filtered.groupby("incident", as_index=False)["record_count"]
            .sum()
            .sort_values("record_count", ascending=True)
            .tail(12)
        )
        fig_incident = px.bar(
            by_incident,
            x="record_count",
            y="incident",
            orientation="h",
            labels={"record_count": "Record Count", "incident": "Incident/Pathologie"},
            color="record_count",
            color_continuous_scale="Blues",
        )
        add_bar_value_labels(fig_incident, orientation="h")
        fig_incident.update_layout(margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False, height=310)
        st.plotly_chart(fig_incident, use_container_width=True)

    with top_row[2]:
        st.subheader("Categories des appels")
        by_category = (
            filtered.groupby("categorie", as_index=False)["record_count"]
            .sum()
            .sort_values("record_count", ascending=True)
        )
        fig_category = px.bar(
            by_category,
            x="record_count",
            y="categorie",
            orientation="h",
            labels={"record_count": "Record Count", "categorie": "Categorie d'appel"},
            color="record_count",
            color_continuous_scale="Tealgrn",
        )
        add_bar_value_labels(fig_category, orientation="h")
        fig_category.update_layout(margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False, height=310)
        st.plotly_chart(fig_category, use_container_width=True)

    st.subheader("Evolution du nombre d'appels")
    daily = group_by_day(filtered, value_col="record_count")
    if daily.empty:
        st.info("Aucune donnee de tendance disponible.")
        return
    fig_daily = px.line(daily, x="date", y="record_count", labels={"record_count": "Record Count", "date": "Date"})
    add_line_value_labels(fig_daily)
    fig_daily.update_traces(line_color=THEME["teal"], marker_size=4)
    fig_daily.update_layout(margin=dict(l=0, r=0, t=15, b=0), height=360)
    st.plotly_chart(fig_daily, use_container_width=True)


def render_alerts_page(alerts_df: pd.DataFrame) -> None:
    if alerts_df.empty:
        st.warning("Aucune donnee d'alertes disponible.")
        return

    locations = sorted(alerts_df["location"].dropna().astype(str).unique().tolist())
    indicators = sorted(alerts_df["indicator"].dropna().astype(str).unique().tolist())

    st.markdown("<div class='filter-title'>Filtres alertes</div>", unsafe_allow_html=True)
    col_f1, col_f2 = st.columns(2)
    selected_locations = col_f1.multiselect("Localite", options=locations, default=locations)
    selected_indicators = col_f2.multiselect("Indicateur", options=indicators, default=indicators)

    filtered = alerts_df.copy()
    if selected_locations:
        filtered = filtered[filtered["location"].isin(selected_locations)]
    if selected_indicators:
        filtered = filtered[filtered["indicator"].isin(selected_indicators)]

    if filtered.empty:
        st.info("Aucune alerte pour les filtres choisis.")
        return

    total_alerts = filtered["value"].sum()
    locations_count = filtered["location"].nunique()
    indicators_count = filtered["indicator"].nunique()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total alertes", format_int(total_alerts))
    c2.metric("Nb localites", format_int(locations_count))
    c3.metric("Nb indicateurs", format_int(indicators_count))

    row = st.columns(2)
    with row[0]:
        st.subheader("Alertes par localite")
        by_location = (
            filtered.groupby("location", as_index=False)["value"]
            .sum()
            .sort_values("value", ascending=True)
            .tail(20)
        )
        fig_loc = px.bar(
            by_location,
            x="value",
            y="location",
            orientation="h",
            labels={"value": "Record Count", "location": "Localite"},
            color="value",
            color_continuous_scale="Blues",
        )
        add_bar_value_labels(fig_loc, orientation="h")
        fig_loc.update_layout(margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_loc, use_container_width=True)

    with row[1]:
        st.subheader("Alertes par indicateur")
        by_indicator = (
            filtered.groupby("indicator", as_index=False)["value"]
            .sum()
            .sort_values("value", ascending=True)
        )
        fig_ind = px.bar(
            by_indicator,
            x="value",
            y="indicator",
            orientation="h",
            labels={"value": "Record Count", "indicator": "Indicateur"},
            color="value",
            color_continuous_scale="Tealgrn",
        )
        add_bar_value_labels(fig_ind, orientation="h")
        fig_ind.update_layout(margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_ind, use_container_width=True)

    st.subheader("Evolution des alertes")
    trend = group_by_day(filtered, value_col="value", category_col="indicator")
    if trend.empty:
        st.info("Aucune donnee de tendance alertes disponible.")
        return
    fig_alert_trend = px.line(
        trend,
        x="date",
        y="value",
        color="indicator",
        labels={"date": "Date", "value": "Record Count", "indicator": "Indicateur"},
    )
    add_line_value_labels(fig_alert_trend)
    fig_alert_trend.update_traces(marker_size=4)
    fig_alert_trend.update_layout(legend_orientation="h", legend_y=1.06, margin=dict(l=0, r=0, t=20, b=0), height=340)
    st.plotly_chart(fig_alert_trend, use_container_width=True)

    st.subheader("Tableau detail des alertes")
    table = (
        filtered.sort_values(["value", "date"], ascending=[False, False])
        .rename(columns={"date": "Date", "location": "Localite", "indicator": "Indicateur", "value": "Record Count", "details": "Details"})
    )
    st.dataframe(table.head(500), use_container_width=True, height=320)


def main() -> None:
    inject_styles()

    st.sidebar.title("Dashboard call center 2025")
    page = st.sidebar.radio(
        "Section",
        ["Informations generales", "Autres details d'informations", "Details alertes"],
        index=0,
    )

    with st.sidebar.expander("Importer les fichiers Excel", expanded=True):
        st.caption("Chargez un ou plusieurs fichiers APPELS et ALERTES avant la visualisation.")
        upload_calls = st.file_uploader(
            "1) APPELS (.xlsx)",
            type=["xls", "xlsx"],
            key="upload_calls",
            accept_multiple_files=True,
        )
        upload_alerts = st.file_uploader(
            "2) ALERTES (.xlsx)",
            type=["xls", "xlsx"],
            key="upload_alerts",
            accept_multiple_files=True,
        )

    calls_df, _calls_info = load_calls_data(upload_calls)
    alerts_df, _alerts_info = load_alerts_data(upload_alerts)

    render_header()

    if not upload_calls or not upload_alerts:
        st.info("Importez au moins un fichier APPELS et un fichier ALERTES dans la barre laterale pour continuer.")
        st.stop()

    kpi_placeholder = st.empty()

    min_date = pd.to_datetime(calls_df["date"], errors="coerce").min()
    max_date = pd.to_datetime(calls_df["date"], errors="coerce").max()
    if pd.isna(min_date) or pd.isna(max_date):
        min_date = pd.Timestamp.today().normalize()
        max_date = min_date

    with st.container(border=True):
        st.markdown("<div class='filter-title'>Zone de filtres principaux</div>", unsafe_allow_html=True)
        filters = st.columns([1.5, 1.1, 1.1, 1.25, 1.25], gap="small")
        date_selected = filters[0].date_input(
            "Selectionner la periode",
            value=(min_date.date(), max_date.date()),
            min_value=min_date.date(),
            max_value=max_date.date(),
        )

        provinces = sorted(calls_df["province"].dropna().astype(str).unique().tolist())
        incidents = sorted(calls_df["incident"].dropna().astype(str).unique().tolist())
        categories = sorted(calls_df["categorie"].dropna().astype(str).unique().tolist())
        genres = sorted(calls_df["genre"].dropna().astype(str).unique().tolist())

        selected_provinces = filters[1].multiselect("Province", provinces, default=provinces)
        selected_genres = filters[2].multiselect("Genre", genres, default=genres)
        selected_incidents = filters[3].multiselect("Incident/Pathologie", incidents, default=incidents)
        selected_categories = filters[4].multiselect("Categorie d'appel", categories, default=categories)

    if isinstance(date_selected, tuple) and len(date_selected) == 2:
        date_start, date_end = date_selected
    else:
        date_start = date_end = date_selected

    filtered_calls = apply_calls_filters(
        calls_df,
        pd.Timestamp(date_start),
        pd.Timestamp(date_end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        selected_provinces,
        selected_genres,
        selected_incidents,
        selected_categories,
    )

    # KPI horizontal dynamique (mis a jour selon les filtres actifs)
    with kpi_placeholder.container():
        render_kpi_horizontal(compute_kpis(filtered_calls))

    start_ts = pd.Timestamp(date_start)
    end_ts = pd.Timestamp(date_end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    window_days = (end_ts.normalize() - start_ts.normalize()).days + 1
    prev_end = start_ts - pd.Timedelta(seconds=1)
    prev_start = prev_end - pd.Timedelta(days=window_days) + pd.Timedelta(seconds=1)

    previous_calls = apply_calls_filters(
        calls_df,
        prev_start,
        prev_end,
        selected_provinces,
        selected_genres,
        selected_incidents,
        selected_categories,
    )
    filtered_alerts = apply_alerts_filters(alerts_df, start_ts, end_ts, selected_provinces)

    render_interactive_analytics(filtered_calls, previous_calls, start_ts, end_ts)

    if page == "Informations generales":
        render_general_page(filtered_calls)
    elif page == "Autres details d'informations":
        render_details_page(filtered_calls)
    else:
        render_alerts_page(filtered_alerts)


if __name__ == "__main__":
    main()
