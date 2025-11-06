// recmar-sync.mjs — RecMar → Shopify bulk inventory sync (GraphQL)
// - DEDUPES by inventory_item_id so batches never include duplicates
// - AUTO-CONNECTS any “not stocked at location” items and retries them
// - Supports optional sku-map.csv (feed_sku,shopify_sku)
// - Writes CSV reports to ./out

/////////////////////////////////////////////////////////////////////////////////
// 1) Preflight
////////////////////////////////////////////////////////////////////////////////
const required = ['SHOPIFY_ADMIN_TOKEN', 'SHOPIFY_STORE'];
const missing = required.filter(k => !process.env[k] || !String(process.env[k]).trim());
if (missing.length) { console.error('❌ Missing required secrets:', missing.join(', ')); process.exit(1); }
if (!process.env.SHOPIFY_LOCATION_ID && !process.env.SHOPIFY_LOCATION_NAME) {
  console.error('❌ Set either SHOPIFY_LOCATION_ID or SHOPIFY_LOCATION_NAME'); process.exit(1);
}

////////////////////////////////////////////////////////////////////////////////
// 2) Env & helpers
////////////////////////////////////////////////////////////////////////////////
const {
  SHOPIFY_ADMIN_TOKEN,
  SHOPIFY_STORE,
  SHOPIFY_API_VERSION = '2025-07',
  SHOPIFY_LOCATION_ID,
  SHOPIFY_LOCATION_NAME,

  // Matching / processing
  FULL_SWEEP = 'true',
  SKU_MATCH_MODE = 'normalize', // "exact" | "prefer-exact" | "normalize"
  GQL_BATCH = '100',
  REPORT_CSV = 'true',
} = process.env;

const ADMIN_BASE = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}`;
const fs = await import('node:fs');
const path = await import('node:path');
const wait = (ms)=>new Promise(r=>setTimeout(r,ms));

function invGid(id){ return `gid://shopify/InventoryItem/${id}`; }
function locGid(id){ return `gid://shopify/Location/${id}`; }
function normalizeSkuText(s){ return String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,''); }

function ensureOutDir(){
  const dir = path.resolve(process.cwd(), 'out');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive:true });
  return dir;
}
function writeCsv(file, rows, header){
  if (!rows?.length) return;
  const lines = [header.join(',')].concat(
    rows.map(r => header.map(h => String(r[h] ?? '').replace(/"/g,'""')).map(v => `"${v}"`).join(','))
  );
  fs.writeFileSync(path.join(ensureOutDir(), file), lines.join('\n'));
}

////////////////////////////////////////////////////////////////////////////////
// 3) Shopify helpers (GraphQL + small REST for connect)
////////////////////////////////////////////////////////////////////////////////
async function gql(query, variables = {}) {
  const res = await fetch(`${ADMIN_BASE}/graphql.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables })
  });
  const body = await res.json().catch(()=>({}));
  if (!res.ok || body.errors) throw new Error('Shopify GraphQL error: ' + JSON.stringify(body));
  return body.data;
}

async function resolveLocationId() {
  if (SHOPIFY_LOCATION_ID) return SHOPIFY_LOCATION_ID;
  const res = await fetch(`${ADMIN_BASE}/locations.json`, {
    headers: { 'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN }
  });
  if (!res.ok) throw new Error('Locations fetch failed: ' + await res.text());
  const data = await res.json();
  const loc = (data.locations || []).find(l => (l.name || '').trim() === SHOPIFY_LOCATION_NAME.trim());
  if (!loc) throw new Error(`Location not found: ${SHOPIFY_LOCATION_NAME}`);
  return String(loc.id);
}

async function fetchAllVariantSkuToInvItem() {
  const exact = new Map();      // exact SKU -> inventory_item_id
  const normalized = new Map(); // normalized SKU -> [inventory_item_id, ...]
  let cursor = null, hasNext = true;
  const q = `
    query($after:String){ productVariants(first:250, after:$after){
      pageInfo{ hasNextPage } edges{ cursor node{ sku inventoryItem{ id } } }
    }}`;
  while (hasNext) {
    const data = await gql(q, { after: cursor });
    const pv = data.productVariants;
    for (const e of pv.edges) {
      const sku = (e.node?.sku || '').trim();
      const gid = e.node?.inventoryItem?.id || '';
      const m = gid.match(/InventoryItem\/(\d+)/);
      if (!sku || !m) { cursor = e.cursor; continue; }
      const invId = m[1];
      exact.set(sku, invId);
      const n = normalizeSkuText(sku);
      const list = normalized.get(n) || [];
      list.push(invId);
      normalized.set(n, list);
      cursor = e.cursor;
    }
    hasNext = pv.pageInfo.hasNextPage;
    await wait(80);
  }
  return { exact, normalized };
}

async function connectInventory(invId, locationId) {
  const res = await fetch(`${ADMIN_BASE}/inventory_levels/connect.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ inventory_item_id: Number(invId), location_id: Number(locationId) })
  });
  if (!res.ok) {
    const txt = await res.text();
    // 422 often means "already connected" — ignore
    if (!/422/.test(String(res.status))) throw new Error(`connect failed: ${res.status} ${txt}`);
  }
}

////////////////////////////////////////////////////////////////////////////////
// 4) Read RecMar numeric CSV from /tmp (created by workflow step)
////////////////////////////////////////////////////////////////////////////////
function parseCsvNumeric(filePath) {
  const txt = fs.readFileSync(filePath, 'utf8').trim();
  const lines = txt.split(/\r?\n/);
  if (!lines.length) return new Map();
  const header = lines[0].split(',');
  const iSku = header.findIndex(h => /^sku$/i.test(h));
  const iQty = header.findIndex(h => /^quantity$/i.test(h));
  if (iSku === -1 || iQty === -1) throw new Error('CSV header must include SKU,Quantity');
  const map = new Map();
  for (let i=1;i<lines.length;i++){
    const row = lines[i].split(',');
    const sku = (row[iSku] || '').trim();
    if (!sku) continue;
    const q = Number(row[iQty] ?? '0');
    if (Number.isNaN(q)) continue;
    map.set(sku, q);
  }
  return map; // sku -> numeric qty
}

////////////////////////////////////////////////////////////////////////////////
// 5) Optional SKU map (sku-map.csv with header: feed_sku,shopify_sku)
////////////////////////////////////////////////////////////////////////////////
function loadSkuMap() {
  try {
    const p = path.resolve(process.cwd(), 'sku-map.csv');
    if (!fs.existsSync(p)) { console.log('sku-map.csv not found (optional)'); return new Map(); }
    const text = fs.readFileSync(p, 'utf8').trim();
    const lines = text.split(/\r?\n/).filter(l => l.trim() && !l.trim().startsWith('#'));
    // skip header if present
    if (/feed_sku/i.test(lines[0]) && /shopify_sku/i.test(lines[0])) lines.shift();
    const map = new Map();
    for (const line of lines) {
      const [feed, shop] = line.split(',');
      if (!feed || !shop) continue;
      const f = feed.trim().replace(/^"|"$/g,'');
      const s = shop.trim().replace(/^"|"$/g,'');
      if (f && s) map.set(f, s);
    }
    console.log(`Loaded sku-map.csv entries: ${map.size}`);
    return map;
  } catch (e) {
    console.warn('Could not load sku-map.csv:', e.message);
    return new Map();
  }
}

////////////////////////////////////////////////////////////////////////////////
// 6) GraphQL bulk writer
////////////////////////////////////////////////////////////////////////////////
const INV_SET_MUT = `
mutation InventorySet($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
    userErrors { field message }
  }
}`;

////////////////////////////////////////////////////////////////////////////////
// 7) Main
////////////////////////////////////////////////////////////////////////////////
(async () => {
  const CSV_PATH = '/tmp/recmar.csv';
  if (!fs.existsSync(CSV_PATH)) {
    console.error(`❌ Expected ${CSV_PATH} to exist. Make sure the workflow step ran.`);
    process.exit(1);
  }
  const feed = parseCsvNumeric(CSV_PATH); // sku -> qty
  console.log(`RecMar CSV rows: ${feed.size}`);

  const { exact, normalized } = await fetchAllVariantSkuToInvItem();
  const location_id = await resolveLocationId();
  const mode = (SKU_MATCH_MODE || 'normalize').toLowerCase();

  // Build matches (exact → mapped → normalized-unique)
  const skuMap = loadSkuMap();
  const candidates = []; // { sku, inventory_item_id, qty }
  const misses = [];
  const ambiguous = [];
  let matchedExact=0, matchedMapped=0, matchedNormalized=0;

  for (const [feedSku, qty] of feed.entries()) {
    let targetSku = feedSku;
    let invId = exact.get(targetSku);

    if (!invId && skuMap.size) {
      const mapped = skuMap.get(feedSku);
      if (mapped) {
        targetSku = mapped;
        invId = exact.get(mapped);
        if (invId) matchedMapped++;
      }
    }

    if (!invId && mode !== 'exact') {
      const nk = normalizeSkuText(targetSku);
      const list = normalized.get(nk) || [];
      if (list.length === 1) {
        invId = list[0];
        matchedNormalized++;
      } else if (list.length > 1) {
        ambiguous.push({ normalized_key: nk, matches: list.length, sample: targetSku });
        continue;
      }
    } else if (invId) {
      matchedExact++;
    }

    if (!invId) {
      misses.push({ sku: feedSku, mapped_to: skuMap.get(feedSku) || '', reason:'not_in_shopify' });
      continue;
    }
    candidates.push({ sku: targetSku, inventory_item_id: String(invId), qty: Number(qty) });
  }

  // DEDUPE by inventory_item_id (last write wins)
  const byInv = new Map();
  for (const x of candidates) byInv.set(x.inventory_item_id, x);
  let work = Array.from(byInv.values());

  // Scope of work
  const BATCH = Math.max(1, Number(GQL_BATCH) || 100);
  if (String(FULL_SWEEP).toLowerCase() !== 'true') {
    work = work.slice(0, 1500); // safety if you ever switch off full sweep
  } else {
    console.log('FULL_SWEEP is ON — processing ALL matched SKUs (GraphQL bulk).');
  }

  console.log(`Shopify variants (unique SKUs): ${exact.size}`);
  console.log(`Matched: exact=${matchedExact}, mapped=${matchedMapped}, normalized=${matchedNormalized}, ambiguous=${ambiguous.length}, misses=${misses.length}`);
  console.log(`Processing this run: ${work.length} (gql_batch=${BATCH})`);

  let updated=0, errors=0;
  const changedRows = [];
  const notStockedSet = new Set(); // inventory_item_id to activate

  // PASS 1 — set quantities
  for (let i=0; i<work.length; i+=BATCH) {
    const chunk = work.slice(i, i+BATCH);
    const input = {
      name: 'available',
      reason: 'correction',
      referenceDocumentUri: 'workhorse://recmar/sync',
      ignoreCompareQuantity: true,
      quantities: chunk.map(x => ({
        inventoryItemId: invGid(x.inventory_item_id),
        locationId: locGid(location_id),
        quantity: Number(x.qty)
      }))
    };
    try {
      const data = await gql(INV_SET_MUT, { input });
      const ue = data?.inventorySetQuantities?.userErrors || [];
      if (ue.length) {
        // Collect “not stocked at the location” items to activate
        for (const e of ue) {
          const msg = String(e.message || '');
          const idx = Number(e.field?.[2] ?? -1);
          if (/not stocked at the location/i.test(msg) && idx >= 0 && idx < chunk.length) {
            notStockedSet.add(chunk[idx].inventory_item_id);
          }
        }
        errors += ue.length;
        console.error('GraphQL userErrors (sample):', ue.slice(0,5));
      }
      const ok = Math.max(0, chunk.length - ue.length);
      updated += ok;
      for (const x of chunk.slice(0, ok)) changedRows.push({ sku: x.sku, inventory_item_id: x.inventory_item_id, to: x.qty });
      await wait(150);
    } catch(e) {
      errors += chunk.length;
      console.error(`GQL batch failed (${i}-${i+BATCH}): ${e.message}`);
      await wait(800);
    }
  }

  // If some items weren’t stocked, CONNECT them and RETRY just those
  if (notStockedSet.size) {
    console.log(`Activating ${notStockedSet.size} inventory items at location ${location_id}…`);
    for (const invId of notStockedSet) {
      try { await connectInventory(invId, location_id); await wait(120); }
      catch (e) { console.warn(`connect failed for ${invId}: ${e.message}`); }
    }

    // Retry setQuantities only for the activated ones
    const retry = work.filter(x => notStockedSet.has(x.inventory_item_id));
    for (let i=0; i<retry.length; i+=BATCH) {
      const chunk = retry.slice(i, i+BATCH);
      const input = {
        name: 'available',
        reason: 'correction',
        referenceDocumentUri: 'workhorse://recmar/sync',
        ignoreCompareQuantity: true,
        quantities: chunk.map(x => ({
          inventoryItemId: invGid(x.inventory_item_id),
          locationId: locGid(location_id),
          quantity: Number(x.qty)
        }))
      };
      try {
        const data = await gql(INV_SET_MUT, { input });
        const ue = data?.inventorySetQuantities?.userErrors || [];
        if (ue.length) {
          errors += ue.length;
          console.error('Retry userErrors (sample):', ue.slice(0,5));
        }
        const ok = Math.max(0, chunk.length - ue.length);
        updated += ok;
        for (const x of chunk.slice(0, ok)) changedRows.push({ sku: x.sku, inventory_item_id: x.inventory_item_id, to: x.qty });
        await wait(150);
      } catch(e) {
        errors += chunk.length;
        console.error(`Retry GQL batch failed (${i}-${i+BATCH}): ${e.message}`);
        await wait(800);
      }
    }
  }

  const summary = {
    feed_rows: feed.size,
    shopify_variants_unique: exact.size,
    matched_exact: matchedExact,
    matched_mapped: matchedMapped,
    matched_normalized: matchedNormalized,
    ambiguous: ambiguous.length,
    misses: misses.length,
    processed_this_run: work.length,
    location_id,
    updated,
    errors,
    full_sweep: String(FULL_SWEEP).toLowerCase() === 'true',
    writer: 'graphql.inventorySetQuantities'
  };
  console.log(summary);

  if (String(REPORT_CSV).toLowerCase() === 'true') {
    writeCsv('changes.csv', changedRows, ['sku','inventory_item_id','to']);
    writeCsv('misses.csv', misses, ['sku','mapped_to','reason']);
    if (ambiguous.length) writeCsv('ambiguous.csv', ambiguous, ['normalized_key','matches','sample']);
    console.log('CSV reports written to ./out (changes.csv, misses.csv, ambiguous.csv).');
  }
})();
