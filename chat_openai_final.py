"""
chat.py  —  POST /chat/ask
Uses OpenAI GPT-4o to translate natural language DBA questions into
safe T-SQL queries, executes them against the selected SQL Server,
and returns results as columns + rows.
"""

import json
import time
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from openai import OpenAI
from core.config import get_settings
from core.database import db_cursor

router = APIRouter(prefix="/chat", tags=["Chat"])
logger = logging.getLogger(__name__)

settings = get_settings()

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are an expert SQL Server DBA assistant embedded in a live monitoring dashboard.

The user will ask plain-English questions about their SQL Server instance.
Your job is to:
1. Understand the question
2. Generate a safe, read-only T-SQL SELECT query using SQL Server DMVs
3. Return a short explanation AND the SQL

CRITICAL RULES:
- ONLY generate SELECT or WITH (CTE) + SELECT statements. NEVER INSERT/UPDATE/DELETE/DROP/EXEC/XP_.
- Always use TOP N to limit rows (default TOP 20 unless user specifies a number).
- Use these DMVs: sys.dm_exec_requests, sys.dm_exec_sessions, sys.dm_exec_query_stats,
  sys.dm_os_wait_stats, sys.dm_os_performance_counters, sys.dm_exec_sql_text,
  sys.dm_db_index_usage_stats, sys.dm_db_missing_index_details, sys.dm_tran_locks.
- Always OUTER APPLY sys.dm_exec_sql_text(sql_handle) to get SQL text.
- Always cast text columns: CAST(st.text AS NVARCHAR(MAX)) — never use ntext directly.
- Always wrap text: CAST(SUBSTRING(CAST(st.text AS NVARCHAR(MAX)),1,250) AS NVARCHAR(250)).
- For CPU queries use sys.dm_exec_query_stats ORDER BY total_worker_time DESC.
- For blocking use sys.dm_exec_requests WHERE blocking_session_id > 0.
- For waits use sys.dm_os_wait_stats excluding benign waits.
- NEVER reference column aliases in WHERE or ORDER BY — use original column expressions.
- When ordering by computed column wrap query in a CTE or subquery first.
- For session wait times ORDER BY r.wait_time DESC not by an alias.
- sys.dm_exec_query_stats does NOT have a database_id column — never use it there.

RESPONSE FORMAT — respond ONLY with valid JSON, no markdown fences, no extra text:
{
  "explanation": "One or two sentences describing what the query does or what was found.",
  "sql": "SELECT TOP 10 ... your T-SQL here ...",
  "title": "Short 3-6 word title for the result table"
}
"""


def ask_openai(question: str) -> dict:
    """Send question to OpenAI GPT-4o and get back explanation + SQL as JSON."""
    if not settings.openai_api_key:
        raise ValueError("OPENAI_API_KEY is not set in .env")

    client = OpenAI(api_key=settings.openai_api_key)

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1024,
        response_format={"type": "json_object"},  # forces pure JSON output
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": question},
        ],
    )

    raw = response.choices[0].message.content.strip()
    return json.loads(raw)


def run_sql(server_id: str, sql: str, max_rows: int = 100) -> dict:
    """Safety-check and execute the AI-generated SQL against the selected server."""
    upper = sql.strip().upper()
    blocked = ["INSERT","UPDATE","DELETE","DROP","TRUNCATE","ALTER","CREATE","EXEC ","EXECUTE ","XP_"]
    for kw in blocked:
        if kw in upper:
            raise ValueError(f"Unsafe keyword detected: {kw}")

    start = time.perf_counter()
    with db_cursor(server_id) as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.execute(sql)
        columns  = [d[0] for d in cursor.description] if cursor.description else []
        raw_rows = cursor.fetchmany(max_rows)
    elapsed = round((time.perf_counter() - start) * 1000, 1)

    rows = []
    for raw in raw_rows:
        row = []
        for cell in raw:
            if cell is None:
                row.append(None)
            elif hasattr(cell, "isoformat"):
                row.append(cell.isoformat())
            elif not isinstance(cell, (int, float, bool)):
                row.append(str(cell))
            else:
                row.append(cell)
        rows.append(row)

    return {
        "columns":      columns,
        "rows":         rows,
        "row_count":    len(rows),
        "execution_ms": elapsed,
    }


# ── Request / response models ─────────────────────────────────────────────────
class ChatRequest(BaseModel):
    server_id: str
    question:  str
    max_rows:  int = 50


class ChatResponse(BaseModel):
    question:     str
    explanation:  str
    title:        str
    sql:          str
    columns:      list[str]
    rows:         list[list]
    row_count:    int
    execution_ms: float


@router.post("/ask", response_model=ChatResponse)
def ask(body: ChatRequest):
    """
    Natural-language DBA question → OpenAI GPT-4o generates SQL → execute → return results.
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    # ── Call OpenAI ───────────────────────────────────────────────────────────
    try:
        ai = ask_openai(body.question)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="OpenAI returned invalid JSON. Try rephrasing.")
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        raise HTTPException(status_code=502, detail=f"OpenAI API error: {e}")

    # ── Execute SQL ───────────────────────────────────────────────────────────
    try:
        result = run_sql(body.server_id, ai["sql"], max_rows=body.max_rows)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"SQL execution error [{body.server_id}]: {e}")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}")

    return ChatResponse(
        question=body.question,
        explanation=ai.get("explanation", ""),
        title=ai.get("title", "Query Result"),
        sql=ai["sql"],
        **result,
    )
