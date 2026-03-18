// ═══════════════════════════════════════════════════════════════════
// sql-editor.js — Core state, constants, utilities, initialization
// ═══════════════════════════════════════════════════════════════════

const SQL_KW = 'SELECT FROM WHERE JOIN LEFT RIGHT INNER OUTER FULL CROSS NATURAL ON AS AND OR NOT IN EXISTS BETWEEN LIKE ILIKE SIMILAR IS NULL TRUE FALSE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE INDEX VIEW DROP ALTER ADD COLUMN PRIMARY KEY FOREIGN REFERENCES UNIQUE DEFAULT CONSTRAINT CHECK GROUP BY ORDER HAVING LIMIT OFFSET UNION EXCEPT INTERSECT ALL DISTINCT WITH RECURSIVE CASE WHEN THEN ELSE ELSIF ELSEIF END CAST OVER PARTITION ROW_NUMBER RANK DENSE_RANK TRUNCATE BEGIN COMMIT ROLLBACK TRANSACTION SAVEPOINT EXPLAIN ANALYZE VACUUM RETURNING COALESCE NULLIF GREATEST LEAST EXTRACT DATE_TRUNC ARRAY JSONB JSON TEXT INTEGER BIGINT SMALLINT INT INT2 INT4 INT8 REAL FLOAT FLOAT4 FLOAT8 DOUBLE PRECISION BOOLEAN NUMERIC DECIMAL CHAR VARCHAR BPCHAR BYTEA XML MONEY BIT VARBIT TSVECTOR TSQUERY OID TIMESTAMP TIMESTAMPTZ DATE TIME TIMETZ INTERVAL SERIAL SMALLSERIAL BIGSERIAL UUID RECORD ROW VOID SETOF DO LANGUAGE PLPGSQL FUNCTION PROCEDURE TRIGGER RETURNS RETURN DECLARE RAISE NOTICE WARNING INFO DEBUG EXCEPTION PERFORM EXECUTE OPEN CLOSE FETCH MOVE LOOP WHILE FOR FOREACH CONTINUE EXIT FOUND SQLSTATE IF SCHEMA DATABASE SEQUENCE EXTENSION ENUM TYPE DOMAIN REPLACE OWNER RENAME CASCADE RESTRICT NO ACTION MATCH SIMPLE PARTIAL MATERIALIZED CONCURRENTLY COMMENT COPY LOCK GRANT REVOKE SUPERUSER CREATEDB CREATEROLE NOINHERIT INHERIT PASSWORD TABLESPACE UNLOGGED TEMP TEMPORARY GLOBAL LOCAL INHERITS GENERATED ALWAYS STORED IDENTITY CYCLE MINVALUE MAXVALUE INCREMENT START CACHE OWNED USING INCLUDE EXCLUDE DEFERRABLE INITIALLY DEFERRED IMMEDIATE RESET SHOW CALL'.split(' ');
const SQL_FN = 'COUNT SUM AVG MIN MAX ROUND FLOOR CEIL ABS MOD POWER SQRT LOG LN EXP SIGN TRUNC LENGTH TRIM UPPER LOWER SUBSTR SUBSTRING REPLACE SPLIT_PART CONCAT CONCAT_WS OVERLAY POSITION LPAD RPAD REPEAT REVERSE INITCAP LEFT RIGHT LTRIM RTRIM BTRIM QUOTE_IDENT QUOTE_LITERAL FORMAT MD5 ENCODE DECODE CONVERT COALESCE NULLIF IFNULL NOW CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP LOCALTIME LOCALTIMESTAMP TRANSACTION_TIMESTAMP CLOCK_TIMESTAMP TIMEOFDAY CURRENT_USER SESSION_USER CURRENT_ROLE TO_CHAR TO_DATE TO_TIMESTAMP TO_NUMBER AGE EXTRACT DATE_TRUNC DATE_PART MAKE_DATE MAKE_TIME MAKE_TIMESTAMP MAKE_INTERVAL JUSTIFY_DAYS JUSTIFY_HOURS JUSTIFY_INTERVAL ARRAY_AGG ARRAY_LENGTH ARRAY_NDIMS ARRAY_UPPER ARRAY_LOWER ARRAY_APPEND ARRAY_PREPEND ARRAY_REMOVE ARRAY_REPLACE ARRAY_POSITION ARRAY_POSITIONS ARRAY_CAT ARRAY_DIMS STRING_AGG JSON_AGG JSON_OBJECT_AGG JSONB_AGG JSONB_OBJECT_AGG ROW_TO_JSON JSON_BUILD_OBJECT JSON_BUILD_ARRAY JSONB_BUILD_OBJECT JSONB_BUILD_ARRAY JSON_EACH JSONB_EACH JSON_EACH_TEXT JSONB_EACH_TEXT JSONB_PATH_EXISTS JSONB_PATH_QUERY JSONB_PATH_QUERY_ARRAY JSON_ARRAY_LENGTH JSONB_ARRAY_LENGTH UNNEST GENERATE_SERIES GENERATE_SUBSCRIPTS REGEXP_REPLACE REGEXP_MATCH REGEXP_MATCHES REGEXP_SPLIT_TO_ARRAY REGEXP_SPLIT_TO_TABLE LEAD LAG FIRST_VALUE LAST_VALUE NTH_VALUE NTILE CUME_DIST PERCENT_RANK ROW_NUMBER RANK DENSE_RANK PG_TYPEOF OID_RECV WIDTH_BUCKET RANDOM SETSEED GREATEST LEAST NULLIF'.split(' ').filter((v,i,a)=>a.indexOf(v)===i);
const SNIPS = {
    select:  'SELECT *\nFROM table_name\nWHERE 1=1\nLIMIT 100;',
    insert:  'INSERT INTO table_name (col1, col2)\nVALUES (val1, val2)\nRETURNING *;',
    update:  'UPDATE table_name\nSET col1 = val1,\n    col2 = val2\nWHERE condition\nRETURNING *;',
    delete:  'DELETE FROM table_name\nWHERE condition\nRETURNING *;',
    explain: 'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)\nSELECT *\nFROM table_name\nWHERE condition;',
    cte:     'WITH cte AS (\n    SELECT *\n    FROM table_name\n    WHERE condition\n)\nSELECT *\nFROM cte\nLIMIT 100;'
};

const S = {
    history: JSON.parse(localStorage.getItem('sqlhist3') || '[]'),
    tables: [],
    master: [], display: [], cols: [],
    sortCol: null, sortDir: 'asc',
    colFilters: {},
    activeTab: 'data',
    acIdx: -1, acItems: [],
    currentEditorTab: 'tab-1',
    tabCounter: 1,
    tabs: {
        'tab-1': { type: 'sql', title: 'SQL Запрос', data: {} }
    },
    colCache: {},
    typeCache: {},
    joinCache: {},
    connInfo: '',
    _acLoading: null,
    _fnList: null,
    _procList: null
};

const MAX_HISTORY_ITEMS = 100;

// ═══ UTILITIES ═══
function eh(s) {
    if (s == null) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function showNotice(msg, type) {
    const d = document.createElement('div');
    const isOk = type !== 'err';
    d.style.cssText = 'position:fixed;bottom:24px;right:24px;z-index:9999;padding:9px 18px;border-radius:5px;font-size:12px;font-family:var(--font-mono);box-shadow:0 4px 16px rgba(0,0,0,.4);font-weight:600;pointer-events:none;transition:opacity .3s;' + (isOk ? 'background:var(--green,#2dba6e);color:#0d0d0d;' : 'background:var(--red,#e04545);color:#fff;');
    d.textContent = msg;
    document.body.appendChild(d);
    setTimeout(() => { d.style.opacity = '0'; setTimeout(() => d.remove(), 300); }, 2200);
}

// ── fetchWithRetry — retry с backoff при сетевых ошибках ──
async function fetchWithRetry(url, opts = {}, retries = 2) {
    for (let i = 0; i <= retries; i++) {
        try { return await fetch(url, opts); }
        catch(e) {
            if (i === retries) throw e;
            await new Promise(r => setTimeout(r, 500 * (i + 1)));
        }
    }
}

// ── Сохранение содержимого вкладок между сессиями ──
function saveTabContent(tabId, value) {
    try { localStorage.setItem('sqltab_' + tabId, value); } catch(e) {}
}
function loadTabContent(tabId) {
    try { return localStorage.getItem('sqltab_' + tabId); } catch(e) { return null; }
}
function clearTabContent(tabId) {
    try { localStorage.removeItem('sqltab_' + tabId); } catch(e) {}
}

// ═══ CONNECTION CHECK ═══
async function checkConn() {
    try {
        const r = await fetch('/api/db-profiles/current');
        const d = await r.json();
        const ov = document.getElementById('notSupportedOverlay');
        const badge = document.getElementById('editorConnectionBadge');
        if (!d.profile) { ov.classList.add('show'); return; }
        const p = d.profile;
        if ((p.db_type || 'postgresql').toLowerCase() !== 'postgresql') { ov.classList.add('show'); return; }
        ov.classList.remove('show');
        const connStr = `${p.ssh_enabled?'🔒 ':''}${p.user}@${p.host}:${p.port}/${p.database}`;
        badge.textContent = connStr;
        badge.style.display = 'block';
        S.connInfo = connStr;
    } catch(e) {
        console.error('[checkConn]', e);
    }
}

// ═══ SPLITTERS ═══
function initSplitters() {
    if (!window.__sqlSplitDrag) window.__sqlSplitDrag = { active: false };

    const wireV = (scope) => {
        const hr = scope.querySelector('#hResizer, .hResizer');
        const ep = scope.querySelector('#editorPane, .editorPane');
        const vs = scope.querySelector('#vSplit, .vSplit');
        if (!hr || !ep || !vs) return;
        if (hr._wired) return;
        hr._wired = true;

        hr.addEventListener('mousedown', e => {
            window.__sqlSplitDrag = {
                active: true,
                startY: e.clientY,
                startH: ep.offsetHeight,
                ep,
                vs
            };
            document.body.style.cursor = 'ns-resize';
            document.body.style.userSelect = 'none';
            e.preventDefault();
        });
    };

    document.querySelectorAll('.tab-content').forEach(wireV);

    if (!window.__sqlSplitDragListenersWired) {
        window.__sqlSplitDragListenersWired = true;
        document.addEventListener('mousemove', e => {
            const d = window.__sqlSplitDrag;
            if (!d || !d.active) return;
            const newH = Math.max(60, Math.min(d.startH + e.clientY - d.startY, d.vs.offsetHeight - 80));
            d.ep.style.height = newH + 'px';
            syncScroll();
            const editor = monacoEditors[S.currentEditorTab];
            if (editor) editor.layout();
        });
        document.addEventListener('mouseup', () => {
            const d = window.__sqlSplitDrag;
            if (d) d.active = false;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        });
    }

    const ts = document.getElementById('treeSplitter'),
          tree = document.getElementById('objectTree');
    if (!ts || !tree) return;

    let dT = false, sX = 0, sW = 0;

    ts.addEventListener('mousedown', e => {
        dT = true;
        sX = e.clientX;
        sW = tree.offsetWidth;
        document.body.style.cursor = 'ew-resize';
        document.body.style.userSelect = 'none';
    });

    document.addEventListener('mousemove', e => {
        if (!dT) return;
        tree.style.width = Math.max(140, Math.min(sW + e.clientX - sX, 500)) + 'px';
    });

    document.addEventListener('mouseup', () => {
        dT = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    });
}

// ═══ MAIN INITIALIZATION ═══
document.addEventListener('DOMContentLoaded', () => {
    // Lock layout width to initial viewport clientWidth (excludes scrollbars)
    const applyAppVw = () => {
        const vw = document.documentElement.clientWidth;
        document.documentElement.style.setProperty('--app-vw', vw + 'px');
    };
    applyAppVw();
    window.addEventListener('resize', () => requestAnimationFrame(applyAppVw), { passive: true });

    _domContentLoaded = true;
    checkConn();
    initSplitters();
    buildTree();
    document.addEventListener('click', e => {
        if (!e.target.closest('#acMenu') && !e.target.closest('.sql-input')) hideAC();
    });
    // Track Ctrl key for visual hint on tree nodes
    document.addEventListener('keydown', e => { if (e.ctrlKey || e.metaKey) document.body.classList.add('ctrl-held'); });
    document.addEventListener('keyup', e => { if (!e.ctrlKey && !e.metaKey) document.body.classList.remove('ctrl-held'); });
    document.addEventListener('blur', () => document.body.classList.remove('ctrl-held'));
    // Wire initial tab-1
    const tab1El = document.querySelector('.tab[data-tab-id="tab-1"]');
    if (tab1El) {
        const sp = tab1El.querySelector('span:not(.tab-close)');
        if (sp) sp.classList.add('tab-title');
        wireTabRename(tab1El, 'tab-1');
        tab1El.addEventListener('mousedown', e => { if (e.button === 1) e.preventDefault(); });
    }
    _tryAfterBothReady();
});
