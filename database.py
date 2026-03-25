"""
database.py — Gerenciamento da base local de EAN via SQLite.
"""

import sqlite3
from datetime import datetime
from typing import Dict, Optional, Tuple

DB_PATH = "ean_database.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db() -> None:
    """Cria a tabela de EANs se não existir."""
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ean_base (
                sku         TEXT PRIMARY KEY,
                ean         TEXT NOT NULL,
                descricao   TEXT DEFAULT '',
                criado_em   TEXT,
                atualizado_em TEXT
            )
        """)
        conn.commit()


def buscar_ean(sku: str) -> Optional[str]:
    """Retorna o EAN para um SKU ou None se não encontrado."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT ean FROM ean_base WHERE sku = ?", (sku,)
        ).fetchone()
    return row[0] if row else None


def get_all_eans() -> Dict[str, str]:
    """Retorna dicionário {sku: ean} com toda a base."""
    with _conn() as conn:
        rows = conn.execute("SELECT sku, ean FROM ean_base").fetchall()
    return {r[0]: r[1] for r in rows}


def upsert_eans(registros: Dict[str, Dict]) -> Tuple[int, int, int]:
    """
    Insere ou atualiza registros em lote.
    registros: {sku: {"ean": "...", "descricao": "..."}}
    Retorna (inseridos, atualizados, erros).
    """
    inseridos = atualizados = erros = 0
    now = datetime.now().isoformat()
    with _conn() as conn:
        for sku, dados in registros.items():
            try:
                ean = str(dados.get("ean", "")).strip()
                desc = str(dados.get("descricao", "")).strip()
                if not sku or not ean:
                    erros += 1
                    continue
                existente = conn.execute(
                    "SELECT sku FROM ean_base WHERE sku = ?", (sku,)
                ).fetchone()
                if existente:
                    conn.execute(
                        "UPDATE ean_base SET ean=?, descricao=?, atualizado_em=? WHERE sku=?",
                        (ean, desc, now, sku),
                    )
                    atualizados += 1
                else:
                    conn.execute(
                        "INSERT INTO ean_base (sku, ean, descricao, criado_em, atualizado_em) VALUES (?,?,?,?,?)",
                        (sku, ean, desc, now, now),
                    )
                    inseridos += 1
            except Exception:
                erros += 1
        conn.commit()
    return inseridos, atualizados, erros


def salvar_ean_manual(sku: str, ean: str, descricao: str = "") -> None:
    """Salva um único EAN manualmente (upsert)."""
    now = datetime.now().isoformat()
    with _conn() as conn:
        existente = conn.execute(
            "SELECT criado_em FROM ean_base WHERE sku = ?", (sku,)
        ).fetchone()
        criado = existente[0] if existente else now
        conn.execute(
            "INSERT OR REPLACE INTO ean_base (sku, ean, descricao, criado_em, atualizado_em) VALUES (?,?,?,?,?)",
            (sku, ean, descricao, criado, now),
        )
        conn.commit()


def get_db_stats() -> Dict:
    """Retorna estatísticas da base."""
    with _conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM ean_base").fetchone()[0]
        ultima = conn.execute(
            "SELECT MAX(atualizado_em) FROM ean_base"
        ).fetchone()[0]
    return {"total": total, "ultima_atualizacao": ultima or "—"}


def listar_todos() -> list:
    """Retorna lista de todos os registros para exibição."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT sku, ean, descricao, atualizado_em FROM ean_base ORDER BY atualizado_em DESC"
        ).fetchall()
    return [
        {"SKU": r[0], "EAN": r[1], "Descrição": r[2], "Atualizado em": r[3]}
        for r in rows
    ]
