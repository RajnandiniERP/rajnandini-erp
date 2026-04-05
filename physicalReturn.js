// ═══════════════════════════════════════════════════════════════
//  PHYSICAL RETURN — Google Sheet Integration
//  File: physicalReturn.js
//  Google Sheet CSV URL (change this if sheet changes):
// ═══════════════════════════════════════════════════════════════

const PR_CSV_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRMKEGmonrd2asd90ls_NFddbCJ-cLKD2Wo8OpG2xK2OhkjSDomAIYfRcmmi5ypYDQF_HUpVf7KNS2m/pub?output=csv';

let prAllRows = [];  // In-memory cache — cleared on page reload


// ── LOAD (called on Refresh button click) ────────────────────────────────────
async function loadPhysicalReturn(forceRefresh) {

  // If data already in memory and not forced, just re-render
  if (!forceRefresh && prAllRows.length > 0) {
    renderPRTable(prAllRows);
    return;
  }

  // First time visit (no force) — show prompt instead of auto-fetching
  if (!forceRefresh && prAllRows.length === 0) {
    document.getElementById('prLoading').style.display = 'none';
    document.getElementById('prError').style.display = 'none';
    document.getElementById('prTableWrap').style.display = 'none';
    document.getElementById('prSummaryRow').innerHTML = '';
    document.getElementById('prCount').innerHTML =
      '<span style="color:var(--muted);font-size:0.78rem;">👆 Click <b>🔄 Refresh from Google Sheet</b> to load data</span>';
    return;
  }

  // ── Reset UI ──────────────────────────────────────────────────
  document.getElementById('prError').style.display = 'none';
  document.getElementById('prTableWrap').style.display = 'none';
  document.getElementById('prSummaryRow').innerHTML = '';
  document.getElementById('prCount').innerHTML = '';
  document.getElementById('prLoading').style.display = '';

  const btn = document.getElementById('prRefreshBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Loading…';

  // ── Progress bar helpers ──────────────────────────────────────
  const bar   = document.getElementById('prProgressBar');
  const pct   = document.getElementById('prProgressPct');
  const label = document.getElementById('prProgressLabel');

  function setProgress(p, msg) {
    bar.style.width = p + '%';
    pct.textContent = p + '%';
    if (msg) label.textContent = msg;
  }

  // Fake ticker: crawls 0 → 28% while network request is pending
  setProgress(0, 'Connecting to Google Sheet…');
  let fake = 0;
  const ticker = setInterval(() => {
    if (fake < 28) { fake += 2; setProgress(fake); }
  }, 80);

  try {
    // ── Step 1: Fetch CSV ─────────────────────────────────────
    const res = await fetch(PR_CSV_URL + '&t=' + Date.now());
    if (!res.ok) throw new Error('HTTP ' + res.status);

    clearInterval(ticker);
    setProgress(40, 'Download complete, reading data…');

    // ── Step 2: Read text ────────────────────────────────────
    const text = await res.text();
    setProgress(60, 'Parsing rows…');

    // ── Step 3: Parse CSV ────────────────────────────────────
    await new Promise(r => setTimeout(r, 120));
    prAllRows = parseCSVtoRows(text);
    setProgress(80, 'Rendering table…');

    // ── Step 4: Render ───────────────────────────────────────
    await new Promise(r => setTimeout(r, 100));
    renderPRTable(prAllRows);
    setProgress(100, 'Done!');

    // Brief pause to show 100%, then hide bar
    await new Promise(r => setTimeout(r, 350));
    document.getElementById('prLoading').style.display = 'none';

    // Update last-synced timestamp
    const now = new Date();
    document.getElementById('prLastSync').textContent =
      'Last synced: ' + now.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' });

    showToast('Physical Return loaded — ' + prAllRows.length + ' rows', 'success');

  } catch (e) {
    clearInterval(ticker);
    document.getElementById('prLoading').style.display = 'none';
    document.getElementById('prError').style.display = '';
    document.getElementById('prErrorMsg').textContent = 'Failed to fetch Google Sheet: ' + e.message;
    showToast('Error loading Physical Return data', 'error');

  } finally {
    btn.disabled = false;
    btn.innerHTML = '🔄 Refresh from Google Sheet';
  }
}


// ── CSV PARSER ───────────────────────────────────────────────────────────────
function parseCSVtoRows(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];

  const header = parseCSVLine(lines[0]);

  // Fuzzy column mapping — matches header names case-insensitively
  const colMap = {};
  const targets = {
    date:         ['date'],
    channel:      ['channel'],
    order_no:     ['order no', 'order no.', 'orderno', 'order_no', 'order number'],
    awb:          ['awb'],
    courier:      ['courier'],
    putway:       ['putway', 'putaway'],
    sku_r:        ['skur', 'sku r', 'sku_r', 'sku'],
    to_rma:       ['to / rma', 'to/rma', 'to_rma', 'rma', 'to'],
    remark:       ['remark', 'remarks'],
    putaway_code: ['putaway code', 'putawaycode', 'putaway_code'],
  };

  header.forEach((h, i) => {
    const hl = h.trim().toLowerCase();
    Object.keys(targets).forEach(key => {
      if (!colMap[key] && targets[key].some(t => hl.includes(t))) colMap[key] = i;
    });
  });

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCSVLine(lines[i]);
    if (cells.every(c => !c.trim())) continue; // skip blank rows
    const get = key => colMap[key] !== undefined ? (cells[colMap[key]] || '').trim() : '';
    rows.push({
      date:         get('date'),
      channel:      get('channel'),
      order_no:     get('order_no'),
      awb:          get('awb'),
      courier:      get('courier'),
      putway:       get('putway'),
      sku_r:        get('sku_r'),
      to_rma:       get('to_rma'),
      remark:       get('remark'),
      putaway_code: get('putaway_code'),
    });
  }
  return rows;
}

// Handles quoted fields with commas inside
function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else { inQ = !inQ; }
    } else if (c === ',' && !inQ) {
      result.push(cur); cur = '';
    } else {
      cur += c;
    }
  }
  result.push(cur);
  return result;
}


// ── RENDER TABLE ─────────────────────────────────────────────────────────────
function renderPRTable(rows) {
  document.getElementById('prLoading').style.display = 'none';
  document.getElementById('prError').style.display = 'none';

  // Rebuild filter dropdowns (keep current selection if still valid)
  const channels = [...new Set(rows.map(r => r.channel).filter(Boolean))].sort();
  const couriers = [...new Set(rows.map(r => r.courier).filter(Boolean))].sort();
  const chSel = document.getElementById('prChannelFilter');
  const coSel = document.getElementById('prCourierFilter');
  const prevCh = chSel.value, prevCo = coSel.value;
  chSel.innerHTML = '<option value="">All Channels</option>' +
    channels.map(c => `<option${c === prevCh ? ' selected' : ''}>${c}</option>`).join('');
  coSel.innerHTML = '<option value="">All Couriers</option>' +
    couriers.map(c => `<option${c === prevCo ? ' selected' : ''}>${c}</option>`).join('');

  filterPR();
}


// ── FILTER + DISPLAY ─────────────────────────────────────────────────────────
function filterPR() {
  const q  = (document.getElementById('prSearch').value  || '').toLowerCase();
  const ch = (document.getElementById('prChannelFilter').value || '').toLowerCase();
  const co = (document.getElementById('prCourierFilter').value || '').toLowerCase();

  const filtered = prAllRows.filter(r => {
    if (ch && r.channel.toLowerCase() !== ch) return false;
    if (co && r.courier.toLowerCase() !== co) return false;
    if (q) {
      const hay = [r.date, r.channel, r.order_no, r.awb, r.courier,
                   r.putway, r.sku_r, r.to_rma, r.remark, r.putaway_code]
                  .join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });

  // Summary chips
  const total        = filtered.length;
  const uniqChannels = [...new Set(filtered.map(r => r.channel).filter(Boolean))].length;
  const uniqCouriers = [...new Set(filtered.map(r => r.courier).filter(Boolean))].length;
  const uniqSKUs     = [...new Set(filtered.map(r => r.sku_r).filter(Boolean))].length;

  document.getElementById('prSummaryRow').innerHTML = `
    <div class="pr-chip">📦 <span>${total}</span> Rows</div>
    <div class="pr-chip">🛒 <span>${uniqChannels}</span> Channels</div>
    <div class="pr-chip">🚚 <span>${uniqCouriers}</span> Couriers</div>
    <div class="pr-chip">🏷️ <span>${uniqSKUs}</span> SKUs</div>`;

  const tbody = document.getElementById('prTableBody');
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;padding:2rem;color:var(--muted);">No records found</td></tr>';
  } else {
    tbody.innerHTML = filtered.map((r, i) => `
      <tr>
        <td style="color:var(--muted);">${i + 1}</td>
        <td>${r.date}</td>
        <td>${r.channel
          ? `<span style="background:#e8f4f0;color:var(--teal);padding:2px 8px;border-radius:12px;font-size:0.68rem;font-weight:600;">${r.channel}</span>`
          : ''}</td>
        <td style="font-family:monospace;font-size:0.7rem;">${r.order_no}</td>
        <td style="font-family:monospace;font-size:0.7rem;">${r.awb}</td>
        <td>${r.courier}</td>
        <td>${r.putway}</td>
        <td style="font-weight:600;color:var(--teal);">${r.sku_r}</td>
        <td>${r.to_rma}</td>
        <td style="color:var(--muted);font-size:0.7rem;">${r.remark}</td>
        <td style="font-weight:600;">${r.putaway_code}</td>
      </tr>`).join('');
  }

  document.getElementById('prTableWrap').style.display = '';
  document.getElementById('prCount').textContent =
    `Showing ${filtered.length} of ${prAllRows.length} records`;
}
