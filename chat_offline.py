"""
chat.py  —  POST /chat/ask
Offline version — no external API needed.
Maps natural language questions to predefined T-SQL queries using keyword matching.
All queries run against live SQL Server data.
"""

import time
import logging
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from core.database import db_cursor

router = APIRouter(prefix="/chat", tags=["Chat"])
logger = logging.getLogger(__name__)


# ── Predefined query library ──────────────────────────────────────────────────
# Each entry has: keywords to match, title, explanation, and SQL
QUERY_LIBRARY = [

    {
        "keywords": ["top", "cpu", "consuming", "expensive", "heavy", "worker", "processor"],
        "title": "Top CPU Consuming Queries",
        "explanation": "These are the queries consuming the most CPU time since SQL Server last started, pulled from the query stats cache.",
        "sql": """
            SELECT TOP 10
                CAST(qs.total_worker_time / 1000 AS BIGINT)         AS cpu_ms_total,
                CAST(qs.total_worker_time / qs.execution_count / 1000 AS BIGINT) AS cpu_ms_avg,
                qs.execution_count,
                CAST(qs.total_elapsed_time / 1000 AS BIGINT)        AS elapsed_ms_total,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS sql_text,
                CAST(DB_NAME(qs.database_id) AS NVARCHAR(128))      AS database_name
            FROM sys.dm_exec_query_stats qs
            OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            ORDER BY qs.total_worker_time DESC
        """
    },

    {
        "keywords": ["top", "memory", "ram", "grant", "buffer", "buffers"],
        "title": "Top Memory Consuming Queries",
        "explanation": "Queries with the highest memory grants currently active or recently executed.",
        "sql": """
            SELECT TOP 10
                r.session_id,
                CAST(mg.granted_memory_kb / 1024 AS BIGINT)         AS granted_mb,
                CAST(mg.used_memory_kb    / 1024 AS BIGINT)         AS used_mb,
                CAST(mg.requested_memory_kb/1024 AS BIGINT)         AS requested_mb,
                CAST(s.login_name AS NVARCHAR(128))                 AS login_name,
                CAST(DB_NAME(r.database_id) AS NVARCHAR(128))       AS database_name,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS sql_text
            FROM sys.dm_exec_query_memory_grants mg
            JOIN sys.dm_exec_requests r  ON mg.session_id = r.session_id
            JOIN sys.dm_exec_sessions  s ON mg.session_id = s.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
            ORDER BY mg.granted_memory_kb DESC
        """
    },

    {
        "keywords": ["block", "blocking", "blocker", "blocked", "lock", "locking", "deadlock"],
        "title": "Current Blocking Sessions",
        "explanation": "Sessions that are currently blocking other sessions from executing.",
        "sql": """
            SELECT
                r.blocking_session_id                               AS blocker_spid,
                r.session_id                                        AS blocked_spid,
                CAST(bs.login_name  AS NVARCHAR(128))               AS blocker_login,
                CAST(s.login_name   AS NVARCHAR(128))               AS blocked_login,
                CAST(bs.host_name   AS NVARCHAR(128))               AS blocker_host,
                r.wait_time                                         AS wait_time_ms,
                CAST(r.wait_type    AS NVARCHAR(64))                AS wait_type,
                CAST(r.wait_resource AS NVARCHAR(256))              AS blocked_resource,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS blocked_sql
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s   ON r.session_id          = s.session_id
            JOIN sys.dm_exec_sessions bs  ON r.blocking_session_id = bs.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
            WHERE r.blocking_session_id > 0
            ORDER BY r.wait_time DESC
        """
    },

    {
        "keywords": ["long", "running", "slow", "duration", "elapsed", "taking"],
        "title": "Long Running Queries",
        "explanation": "Queries that have been running for more than 30 seconds right now.",
        "sql": """
            SELECT TOP 20
                r.session_id,
                CAST(s.login_name AS NVARCHAR(128))                 AS login_name,
                CAST(s.host_name  AS NVARCHAR(128))                 AS host_name,
                CAST(DB_NAME(r.database_id) AS NVARCHAR(128))       AS database_name,
                DATEDIFF(SECOND, r.start_time, GETDATE())           AS duration_seconds,
                r.cpu_time                                          AS cpu_ms,
                r.logical_reads,
                CAST(r.status    AS NVARCHAR(32))                   AS status,
                CAST(r.wait_type AS NVARCHAR(64))                   AS wait_type,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS sql_text
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
            WHERE s.is_user_process = 1
              AND DATEDIFF(SECOND, r.start_time, GETDATE()) >= 30
            ORDER BY DATEDIFF(SECOND, r.start_time, GETDATE()) DESC
        """
    },

    {
        "keywords": ["wait", "waits", "waiting", "wait stats", "wait statistics", "wait type"],
        "title": "Top Wait Statistics",
        "explanation": "The most significant wait types on this SQL Server since last restart, excluding benign system waits.",
        "sql": """
            SELECT TOP 20
                CAST(wait_type AS NVARCHAR(64))                     AS wait_type,
                waiting_tasks_count,
                CAST(wait_time_ms / 1000.0 AS DECIMAL(18,1))       AS wait_time_sec,
                CAST(max_wait_time_ms/1000.0 AS DECIMAL(18,1))     AS max_wait_sec,
                CAST(
                    100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER(), 0)
                AS DECIMAL(5,2))                                    AS pct_of_total
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN (
                'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP',
                'CLR_AUTO_EVENT','DISPATCHER_QUEUE_SEMAPHORE',
                'FT_IFTS_SCHEDULER_IDLE_WAIT','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                'HADR_WORK_QUEUE','LAZYWRITER_SLEEP','LOGMGR_QUEUE',
                'ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH',
                'RESOURCE_QUEUE','SERVER_IDLE_CHECK','SLEEP_DBSTARTUP',
                'SLEEP_DCOMSTARTUP','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
                'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SLEEP_SYSTEMTASK',
                'SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP',
                'SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                'WAITFOR','XE_DISPATCHER_WAIT','XE_TIMER_EVENT',
                'BROKER_EVENTHANDLER','CHECKPOINT_QUEUE','DBMIRROR_EVENTS_QUEUE',
                'SQLTRACE_WAIT_ENTRIES','WAIT_XTP_OFFLINE_CKPT_NEW_LOG'
            )
            ORDER BY wait_time_ms DESC
        """
    },

    {
        "keywords": ["missing", "index", "indexes", "indices", "recommend", "suggestion"],
        "title": "Top Missing Indexes",
        "explanation": "Indexes that SQL Server recommends creating based on query patterns, ranked by potential performance impact.",
        "sql": """
            SELECT TOP 10
                CAST(
                    qs.avg_total_user_cost * qs.avg_user_impact * (qs.user_seeks + qs.user_scans)
                AS DECIMAL(18,0))                                   AS impact_score,
                CAST(d.statement AS NVARCHAR(256))                  AS table_name,
                CAST(d.equality_columns   AS NVARCHAR(512))         AS equality_columns,
                CAST(d.inequality_columns AS NVARCHAR(512))         AS inequality_columns,
                CAST(d.included_columns   AS NVARCHAR(512))         AS included_columns,
                qs.user_seeks,
                qs.user_scans
            FROM sys.dm_db_missing_index_details d
            JOIN sys.dm_db_missing_index_groups g
                ON d.index_handle = g.index_handle
            JOIN sys.dm_db_missing_index_group_stats qs
                ON g.index_group_handle = qs.group_handle
            ORDER BY impact_score DESC
        """
    },

    {
        "keywords": ["index", "usage", "used", "seeks", "scans", "lookups", "unused"],
        "title": "Index Usage Statistics",
        "explanation": "How often each index is being used — seeks, scans, lookups and updates since last restart.",
        "sql": """
            SELECT TOP 20
                CAST(OBJECT_NAME(i.object_id) AS NVARCHAR(128))     AS table_name,
                CAST(i.name AS NVARCHAR(128))                       AS index_name,
                CAST(i.type_desc AS NVARCHAR(64))                   AS index_type,
                s.user_seeks,
                s.user_scans,
                s.user_lookups,
                s.user_updates,
                s.user_seeks + s.user_scans + s.user_lookups        AS total_reads,
                CAST(s.last_user_seek AS NVARCHAR(32))              AS last_seek
            FROM sys.dm_db_index_usage_stats s
            JOIN sys.indexes i
                ON s.object_id = i.object_id
               AND s.index_id  = i.index_id
            WHERE s.database_id = DB_ID()
              AND i.name IS NOT NULL
            ORDER BY total_reads DESC
        """
    },

    {
        "keywords": ["session", "sessions", "active", "connection", "connections", "who", "logged"],
        "title": "Active Sessions",
        "explanation": "All currently active user sessions on this SQL Server instance.",
        "sql": """
            SELECT TOP 30
                s.session_id,
                CAST(s.login_name   AS NVARCHAR(128))               AS login_name,
                CAST(s.host_name    AS NVARCHAR(128))               AS host_name,
                CAST(s.status       AS NVARCHAR(32))                AS status,
                CAST(s.program_name AS NVARCHAR(128))               AS program_name,
                CAST(DB_NAME(r.database_id) AS NVARCHAR(128))       AS database_name,
                ISNULL(r.cpu_time, 0)                               AS cpu_ms,
                ISNULL(r.logical_reads, 0)                          AS logical_reads,
                CAST(r.wait_type AS NVARCHAR(64))                   AS wait_type,
                ISNULL(r.wait_time, 0)                              AS wait_ms
            FROM sys.dm_exec_sessions s
            LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
            WHERE s.is_user_process = 1
            ORDER BY ISNULL(r.cpu_time, 0) DESC
        """
    },

    {
        "keywords": ["read", "reads", "logical", "io", "disk", "physical", "scan"],
        "title": "Top Queries by Logical Reads",
        "explanation": "Queries performing the most logical reads — high logical reads often indicate missing indexes or inefficient queries.",
        "sql": """
            SELECT TOP 10
                CAST(qs.total_logical_reads / qs.execution_count AS BIGINT) AS avg_logical_reads,
                qs.total_logical_reads,
                qs.execution_count,
                CAST(qs.total_worker_time / 1000 AS BIGINT)        AS cpu_ms_total,
                CAST(DB_NAME(qs.database_id) AS NVARCHAR(128))     AS database_name,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS sql_text
            FROM sys.dm_exec_query_stats qs
            OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            ORDER BY qs.total_logical_reads DESC
        """
    },

    {
        "keywords": ["execution", "executions", "frequent", "called", "most run", "ran"],
        "title": "Most Frequently Executed Queries",
        "explanation": "Queries executed most often — useful for identifying candidates for caching or optimisation.",
        "sql": """
            SELECT TOP 10
                qs.execution_count,
                CAST(qs.total_worker_time / qs.execution_count / 1000 AS BIGINT) AS avg_cpu_ms,
                CAST(qs.total_elapsed_time/ qs.execution_count / 1000 AS BIGINT) AS avg_elapsed_ms,
                CAST(qs.total_logical_reads/ qs.execution_count AS BIGINT)       AS avg_reads,
                CAST(DB_NAME(qs.database_id) AS NVARCHAR(128))     AS database_name,
                CAST(SUBSTRING(
                    CAST(st.text AS NVARCHAR(MAX)), 1, 200
                ) AS NVARCHAR(200))                                  AS sql_text
            FROM sys.dm_exec_query_stats qs
            OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            ORDER BY qs.execution_count DESC
        """
    },

    {
        "keywords": ["database", "databases", "size", "space", "disk", "storage"],
        "title": "Database Sizes",
        "explanation": "Size of all databases on this SQL Server instance including data and log file sizes.",
        "sql": """
            SELECT
                CAST(d.name AS NVARCHAR(128))                       AS database_name,
                CAST(d.state_desc AS NVARCHAR(32))                  AS status,
                CAST(d.recovery_model_desc AS NVARCHAR(32))         AS recovery_model,
                CAST(SUM(CASE WHEN mf.type = 0 THEN mf.size END)
                    * 8 / 1024.0 AS DECIMAL(18,1))                  AS data_mb,
                CAST(SUM(CASE WHEN mf.type = 1 THEN mf.size END)
                    * 8 / 1024.0 AS DECIMAL(18,1))                  AS log_mb,
                CAST(SUM(mf.size) * 8 / 1024.0 AS DECIMAL(18,1))   AS total_mb
            FROM sys.databases d
            JOIN sys.master_files mf ON d.database_id = mf.database_id
            GROUP BY d.name, d.state_desc, d.recovery_model_desc
            ORDER BY total_mb DESC
        """
    },

    {
        "keywords": ["job", "jobs", "agent", "schedule", "scheduled", "failed", "failure"],
        "title": "SQL Agent Job Status",
        "explanation": "Recent SQL Server Agent job execution history showing success and failure status.",
        "sql": """
            SELECT TOP 20
                CAST(j.name AS NVARCHAR(128))                       AS job_name,
                CAST(CASE h.run_status
                    WHEN 0 THEN 'Failed'
                    WHEN 1 THEN 'Succeeded'
                    WHEN 2 THEN 'Retry'
                    WHEN 3 THEN 'Cancelled'
                    ELSE 'Unknown'
                END AS NVARCHAR(32))                                AS last_status,
                CAST(CONVERT(NVARCHAR, h.run_date) AS NVARCHAR(16)) AS run_date,
                CAST(CONVERT(NVARCHAR, h.run_time) AS NVARCHAR(16)) AS run_time,
                h.run_duration                                      AS duration_hhmmss,
                CAST(j.enabled AS INT)                              AS is_enabled
            FROM msdb.dbo.sysjobs j
            LEFT JOIN msdb.dbo.sysjobhistory h
                ON j.job_id = h.job_id
               AND h.step_id = 0
            ORDER BY h.run_date DESC, h.run_time DESC
        """
    },

    {
        "keywords": ["memory", "ram", "buffer", "pool", "ple", "page life"],
        "title": "Memory Usage Overview",
        "explanation": "Current SQL Server memory usage including buffer pool, stolen memory and page life expectancy.",
        "sql": """
            SELECT
                CAST(physical_memory_in_use_kb / 1024.0 AS DECIMAL(18,1))  AS used_mb,
                CAST(page_fault_count AS BIGINT)                            AS page_faults,
                CAST(memory_utilization_percentage AS INT)                  AS utilisation_pct,
                (SELECT CAST(cntr_value AS BIGINT)
                 FROM sys.dm_os_performance_counters
                 WHERE counter_name = 'Page life expectancy'
                   AND object_name LIKE '%Buffer Manager%')                 AS page_life_exp_sec,
                (SELECT CAST(cntr_value AS BIGINT)
                 FROM sys.dm_os_performance_counters
                 WHERE counter_name = 'Memory Grants Pending')              AS grants_pending
            FROM sys.dm_os_process_memory
        """
    },

    {
        "keywords": ["help", "what", "can", "ask", "questions", "list", "commands", "available"],
        "title": "Available Questions",
        "explanation": "Here are all the questions you can ask me. Just type any of these naturally!",
        "sql": """
            SELECT
                CAST(category   AS NVARCHAR(64))  AS category,
                CAST(example    AS NVARCHAR(256)) AS example_question
            FROM (VALUES
                ('Performance',  'Top 3 CPU consuming queries'),
                ('Performance',  'Top queries by logical reads'),
                ('Performance',  'Most frequently executed queries'),
                ('Memory',       'Top memory consuming queries'),
                ('Memory',       'Memory usage overview'),
                ('Blocking',     'Show me all blocking sessions'),
                ('Sessions',     'Who is connected right now?'),
                ('Sessions',     'Show active sessions'),
                ('Long Running', 'Show long running queries'),
                ('Long Running', 'Queries taking more than 30 seconds'),
                ('Waits',        'What are the current wait statistics?'),
                ('Indexes',      'Show missing indexes'),
                ('Indexes',      'Index usage statistics'),
                ('Database',     'Show database sizes'),
                ('Jobs',         'Show SQL Agent job status')
            ) AS t(category, example_question)
            ORDER BY category, example_question
        """
    },
]


# ── Keyword matcher ───────────────────────────────────────────────────────────
def find_best_match(question: str) -> dict | None:
    """
    Score each query in the library by how many keywords match the question.
    Returns the highest scoring match, or None if no match found.
    """
    q = question.lower()
    # tokenise — split on spaces and punctuation
    words = set(re.findall(r'[a-z]+', q))

    best_score = 0
    best_match = None

    for entry in QUERY_LIBRARY:
        score = sum(1 for kw in entry["keywords"] if kw in q or kw in words)
        if score > best_score:
            best_score = score
            best_match = entry

    # Require at least 1 keyword match
    return best_match if best_score >= 1 else None


def run_sql(sql: str, max_rows: int = 100) -> dict:
    """Execute predefined SQL and return columns + rows."""
    start = time.perf_counter()
    with db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description] if cursor.description else []
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
    question: str
    max_rows: int = 50


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
    Natural-language question → keyword match → predefined SQL → live results.
    No external API required.
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    match = find_best_match(body.question)

    if not match:
        raise HTTPException(
            status_code=404,
            detail=(
                "I didn't understand that question. "
                "Try asking: 'top CPU queries', 'blocking sessions', "
                "'missing indexes', 'wait statistics', or type 'help' to see all options."
            )
        )

    try:
        result = run_sql(match["sql"], max_rows=body.max_rows)
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}")

    return ChatResponse(
        question=body.question,
        explanation=match["explanation"],
        title=match["title"],
        sql=match["sql"],
        **result,
    )
