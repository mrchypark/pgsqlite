#!/usr/bin/env python3
"""Targeted PostgreSQL compatibility smoke checks for Phase 2."""

from __future__ import annotations

import argparse
import uuid

import psycopg
from sqlalchemy import create_engine, text


def expect_equal(actual, expected, message: str) -> None:
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def run_psycopg_checks(port: int) -> None:
    role_name = f"compat_ci_role_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(
        host="127.0.0.1",
        port=port,
        user="postgres",
        dbname="main",
    )

    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS compat_users")
            cur.execute(
                """
                CREATE TABLE compat_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
                """
            )
            conn.commit()

            cur.execute("BEGIN")
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cur.execute("SHOW transaction_isolation")
            show_value = cur.fetchone()[0]
            cur.execute("SELECT current_setting('transaction_isolation')")
            current_setting_value = cur.fetchone()[0]
            cur.execute(
                "SELECT setting FROM pg_settings WHERE name = 'transaction_isolation'"
            )
            pg_settings_value = cur.fetchone()[0]

            expect_equal(
                show_value,
                "serializable",
                "SHOW transaction_isolation should reflect SET TRANSACTION",
            )
            expect_equal(
                current_setting_value,
                "serializable",
                "current_setting should reflect SET TRANSACTION",
            )
            expect_equal(
                pg_settings_value,
                "serializable",
                "pg_settings should reflect SET TRANSACTION",
            )

            cur.execute("SET LOCAL search_path = pg_catalog")
            cur.execute("SHOW search_path")
            expect_equal(
                cur.fetchone()[0],
                "pg_catalog",
                "SET LOCAL search_path should be visible inside transaction",
            )
            cur.execute("ROLLBACK")

            cur.execute("SHOW search_path")
            post_rollback_search_path = cur.fetchone()[0]
            if post_rollback_search_path == "pg_catalog":
                raise AssertionError(
                    "SET LOCAL search_path leaked past ROLLBACK"
                )

            cur.execute(f"CREATE ROLE {role_name} LOGIN")
            conn.commit()

            cur.execute("SELECT rolname, rolcanlogin FROM pg_roles")
            role_row = next(
                (row for row in cur.fetchall() if row[0] == role_name),
                None,
            )
            if role_row is None:
                raise AssertionError("Created role missing from pg_roles")
            expect_equal(role_row[0], role_name, "pg_roles rolname mismatch")
            if role_row[1] not in (True, "t"):
                raise AssertionError(
                    f"pg_roles rolcanlogin mismatch: expected True/'t', got {role_row[1]!r}"
                )

            cur.execute("SELECT usename FROM pg_user")
            pg_user_names = {row[0] for row in cur.fetchall()}
            if role_name not in pg_user_names:
                raise AssertionError("Created LOGIN role missing from pg_user")

            cur.execute(
                "INSERT INTO compat_users (id, name) VALUES (%s, %s)",
                (1, "alpha"),
            )
            conn.commit()

            cur.execute(
                "SELECT id, name FROM compat_users WHERE id = %s",
                (1,),
            )
            row = cur.fetchone()
            expect_equal(row, (1, "alpha"), "Parameterized SELECT row mismatch")
            description = cur.description
            expect_equal(description[0].name, "id", "SELECT id column name mismatch")
            expect_equal(description[0].type_code, 23, "SELECT id OID mismatch")
            expect_equal(
                description[1].name, "name", "SELECT name column name mismatch"
            )
            expect_equal(description[1].type_code, 25, "SELECT name OID mismatch")

            cur.execute(
                "INSERT INTO compat_users (id, name) VALUES (%s, %s) RETURNING id, name",
                (2, "beta"),
            )
            returning_row = cur.fetchone()
            expect_equal(returning_row, (2, "beta"), "RETURNING row mismatch")
            returning_description = cur.description
            expect_equal(
                returning_description[0].name,
                "id",
                "RETURNING id column name mismatch",
            )
            expect_equal(
                returning_description[0].type_code, 23, "RETURNING id OID mismatch"
            )
            expect_equal(
                returning_description[1].name,
                "name",
                "RETURNING name column name mismatch",
            )
            expect_equal(
                returning_description[1].type_code,
                25,
                "RETURNING name OID mismatch",
            )

            cur.execute(f"DROP ROLE {role_name}")
            cur.execute("DROP TABLE compat_users")
            conn.commit()
    finally:
        conn.close()


def run_sqlalchemy_smoke(port: int) -> None:
    engine = create_engine(
        f"postgresql+psycopg://postgres@127.0.0.1:{port}/main",
        future=True,
    )
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE IF NOT EXISTS compat_reflect_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
                """
            )
            conn.execute(
                text(
                    "INSERT INTO compat_reflect_users (id, name) VALUES (1, 'gamma')"
                )
            )

        with engine.connect() as conn:
            value = conn.execute(
                text("SELECT name FROM compat_reflect_users WHERE id = 1")
            ).scalar_one()
            expect_equal(value, "gamma", "SQLAlchemy text query row mismatch")
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql("DROP TABLE IF EXISTS compat_reflect_users")
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run targeted psycopg3 + SQLAlchemy compatibility smoke checks"
    )
    parser.add_argument("--port", type=int, required=True, help="pgsqlite TCP port")
    args = parser.parse_args()

    run_psycopg_checks(args.port)
    run_sqlalchemy_smoke(args.port)
    print("targeted compatibility smoke checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
