from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from data_dictionary import canonical_pathology_name


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: str
    database: str
    user: str
    password: str
    schema: str
    sslmode: str


def env(key: str, default: str = "") -> str:
    return str(os.getenv(key, default))


def sanitize_identifier(value: str, fallback: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9_]", "_", str(value or "").strip())
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", candidate):
        return candidate.lower()
    return fallback


def pg_url(cfg: PgConfig) -> str:
    return f"postgresql+psycopg://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}?sslmode={cfg.sslmode}"


def parse_json_map(raw: str) -> dict[str, str]:
    if not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in obj.items():
            key = str(k).strip()
            val = str(v).strip()
            if key and val:
                out[key] = val
        return out
    except Exception:
        return {}


def fetch_aggregates(
    cfg: PgConfig,
    start_date: str,
    end_date: str,
    records_table: str = "call_center_records",
) -> dict[str, object]:
    schema = sanitize_identifier(cfg.schema, "public")
    records = sanitize_identifier(records_table, "call_center_records")

    records_ref = f'"{schema}"."{records}"'

    engine = create_engine(pg_url(cfg), pool_pre_ping=True)
    params = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        totals = conn.execute(
            text(
                f"""
                SELECT
                    COALESCE(SUM(record_count), 0) AS total_appels,
                    COALESCE(SUM(CASE WHEN statut = 'Resolu' THEN record_count ELSE 0 END), 0) AS resolu,
                    COALESCE(SUM(CASE WHEN statut <> 'Resolu' THEN record_count ELSE 0 END), 0) AS non_resolu,
                    COALESCE(SUM(CASE WHEN genre = 'Homme' THEN record_count ELSE 0 END), 0) AS hommes,
                    COALESCE(SUM(CASE WHEN genre = 'Femme' THEN record_count ELSE 0 END), 0) AS femmes,
                    COALESCE(SUM(CASE WHEN genre = 'ND' THEN record_count ELSE 0 END), 0) AS nd
                FROM {records_ref}
                WHERE date::date BETWEEN :start_date::date AND :end_date::date
                """
            ),
            params,
        ).mappings().first()

        total_alertes = conn.execute(
            text(
                f"""
                SELECT
                    COALESCE(
                        SUM(
                            CASE
                                WHEN LOWER(COALESCE(categorie, '')) LIKE '%alerte%'
                                     OR COALESCE(source_kind, '') = 'alerts'
                                THEN record_count
                                ELSE 0
                            END
                        ),
                        0
                    ) AS total_alertes
                FROM {records_ref}
                WHERE date::date BETWEEN :start_date::date AND :end_date::date
                """
            ),
            params,
        ).scalar_one()

        categories_df = pd.read_sql_query(
            text(
                f"""
                SELECT categorie, COALESCE(SUM(record_count), 0) AS value
                FROM {records_ref}
                WHERE date::date BETWEEN :start_date::date AND :end_date::date
                GROUP BY categorie
                ORDER BY value DESC
                """
            ),
            conn,
            params=params,
        )

        pathologies_df = pd.read_sql_query(
            text(
                f"""
                SELECT incident, COALESCE(SUM(record_count), 0) AS value
                FROM {records_ref}
                WHERE date::date BETWEEN :start_date::date AND :end_date::date
                GROUP BY incident
                ORDER BY value DESC
                """
            ),
            conn,
            params=params,
        )

    totals_dict = dict(totals or {})
    totals_dict["total_alertes"] = float(total_alertes or 0)
    categories_df["categorie"] = categories_df["categorie"].fillna("Non classe").astype(str)
    pathologies_df["incident"] = pathologies_df["incident"].fillna("Non precise").map(canonical_pathology_name)

    return {
        "totals": totals_dict,
        "categories": categories_df,
        "pathologies": pathologies_df,
    }


def build_data_values(
    period: str,
    org_unit: str,
    coc_uid: str,
    aoc_uid: str,
    aggregates: dict[str, object],
) -> list[dict[str, str]]:
    totals = aggregates["totals"]
    categories_df: pd.DataFrame = aggregates["categories"]
    pathologies_df: pd.DataFrame = aggregates["pathologies"]

    fixed_map = {
        "total_alertes": env("DHIS2_DE_ALERTES_TOTAL"),
        "total_appels": env("DHIS2_DE_APPELS_TOTAL"),
        "resolu": env("DHIS2_DE_RESOLU"),
        "non_resolu": env("DHIS2_DE_NON_RESOLU"),
        "hommes": env("DHIS2_DE_HOMMES"),
        "femmes": env("DHIS2_DE_FEMMES"),
        "nd": env("DHIS2_DE_ND"),
    }
    category_map = parse_json_map(env("DHIS2_CATEGORY_DATAELEMENT_MAP", ""))
    pathology_map = parse_json_map(env("DHIS2_PATHOLOGY_DATAELEMENT_MAP", ""))

    data_values: list[dict[str, str]] = []

    def add_value(uid: str, value: float) -> None:
        if not uid:
            return
        data_values.append(
            {
                "dataElement": uid,
                "period": period,
                "orgUnit": org_unit,
                "categoryOptionCombo": coc_uid,
                "attributeOptionCombo": aoc_uid,
                "value": str(int(round(float(value)))),
            }
        )

    for metric, uid in fixed_map.items():
        add_value(uid, float(totals.get(metric, 0)))

    for _, row in categories_df.iterrows():
        uid = category_map.get(str(row["categorie"]).strip())
        add_value(uid, float(row["value"]))

    for _, row in pathologies_df.iterrows():
        uid = pathology_map.get(str(row["incident"]).strip())
        add_value(uid, float(row["value"]))

    return data_values


def push_to_dhis2(dhis2_url: str, username: str, password: str, data_values: list[dict[str, str]]) -> None:
    if not data_values:
        print("Aucune valeur a envoyer vers DHIS2 (mappings absents ou jeu vide).")
        return

    url = dhis2_url.rstrip("/") + "/api/dataValueSets?importStrategy=CREATE_AND_UPDATE"
    payload = {"dataValues": data_values}
    resp = requests.post(url, auth=(username, password), json=payload, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"DHIS2 import failed [{resp.status_code}] {resp.text}")
    print(f"DHIS2 import OK [{resp.status_code}]")
    print(resp.text)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export des agregats Call Center (PostgreSQL) vers DHIS2 dataValueSets."
    )
    parser.add_argument("--start-date", required=True, help="Date debut YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Date fin YYYY-MM-DD")
    parser.add_argument("--period", default="", help="Periode DHIS2 (ex: 202603, 20260308).")
    args = parser.parse_args()

    # Validation basique des dates.
    datetime.strptime(args.start_date, "%Y-%m-%d")
    datetime.strptime(args.end_date, "%Y-%m-%d")
    period = args.period.strip() or args.end_date.replace("-", "")

    pg_cfg = PgConfig(
        host=env("PGHOST", "localhost"),
        port=env("PGPORT", "5432"),
        database=env("PGDATABASE", "call_center"),
        user=env("PGUSER", "postgres"),
        password=env("PGPASSWORD", ""),
        schema=env("PGSCHEMA", "public"),
        sslmode=env("PGSSLMODE", "prefer"),
    )

    dhis2_url = env("DHIS2_URL")
    dhis2_user = env("DHIS2_USERNAME")
    dhis2_pass = env("DHIS2_PASSWORD")
    dhis2_org = env("DHIS2_ORG_UNIT_UID")
    dhis2_coc = env("DHIS2_DEFAULT_COC_UID", "HllvX50cXC0")
    dhis2_aoc = env("DHIS2_DEFAULT_AOC_UID", "HllvX50cXC0")

    missing = [
        k
        for k, v in {
            "DHIS2_URL": dhis2_url,
            "DHIS2_USERNAME": dhis2_user,
            "DHIS2_PASSWORD": dhis2_pass,
            "DHIS2_ORG_UNIT_UID": dhis2_org,
        }.items()
        if not v
    ]
    if missing:
        raise RuntimeError(f"Variables DHIS2 manquantes: {', '.join(missing)}")

    aggregates = fetch_aggregates(pg_cfg, start_date=args.start_date, end_date=args.end_date)
    data_values = build_data_values(
        period=period,
        org_unit=dhis2_org,
        coc_uid=dhis2_coc,
        aoc_uid=dhis2_aoc,
        aggregates=aggregates,
    )
    print(f"Valeurs construites: {len(data_values)}")
    push_to_dhis2(dhis2_url, dhis2_user, dhis2_pass, data_values)


if __name__ == "__main__":
    main()
