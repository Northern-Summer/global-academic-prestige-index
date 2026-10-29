import './style.css';
import './scores.css';
import './global.js';
import { validateScoreSnapshot, verifySnapshotBytes } from './snapshot.js';

const $ = (selector) => document.querySelector(selector);
const esc = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const pageSize = 50;
let records = [];
let page = 0;
let searchTimer;
let snapshot;

function roleLabel(record) {
  if (record.roles.includes('employer') && record.roles.includes('doctoral_origin')) return 'Employer + doctoral origin';
  if (record.roles.includes('employer')) return 'US faculty employer';
  return 'Doctoral origin only';
}

function renderDirectory() {
  const query = $('#directory-search').value.trim().toLocaleLowerCase();
  const role = $('#directory-role').value;
  const sort = $('#directory-sort').value;
  const filtered = records.filter((record) =>
    (!query || record.name.toLocaleLowerCase().includes(query))
    && (role === 'all' || record.roles.includes(role))
  );
  filtered.sort((left, right) => {
    if (sort === 'name') return left.name.localeCompare(right.name);
    if (sort === 'placements') return right.source_recorded_doctoral_origin_count - left.source_recorded_doctoral_origin_count || left.name.localeCompare(right.name);
    if (sort === 'hires') return right.source_recorded_faculty_count - left.source_recorded_faculty_count || left.name.localeCompare(right.name);
    return left.rank - right.rank;
  });
  const pageCount = Math.max(1, Math.ceil(filtered.length / pageSize));
  page = Math.min(page, pageCount - 1);
  const visible = filtered.slice(page * pageSize, (page + 1) * pageSize);
  $('#directory-rows').innerHTML = visible.map((record) => `<tr><td class="rank-cell"><span class="rank-pill">${esc(record.rank)}</span></td><td><strong>${esc(record.name)}</strong><small>${esc(record.institution_id)}</small></td><td class="score composite-score">${record.gapi_network_score.toFixed(8)}</td><td>${record.source_recorded_doctoral_origin_count.toLocaleString('en')}<small>${record.ranking_network_doctoral_origin_count.toLocaleString('en')} in ranking network</small></td><td>${record.source_recorded_faculty_count.toLocaleString('en')}<small>${record.ranking_network_employer_count.toLocaleString('en')} in ranking network</small></td><td><span class="status ranked">${esc(roleLabel(record))}</span>${record.excluded_external_doctoral_origin_edges ? `<small>${record.excluded_external_doctoral_origin_edges.toLocaleString('en')} external-origin relations excluded (${record.excluded_external_doctoral_origin_count.toLocaleString('en')} source records)</small>` : ''}</td></tr>`).join('') || '<tr><td class="empty-state" colspan="6">No real institutions match these filters.</td></tr>';
  const first = filtered.length ? page * pageSize + 1 : 0;
  const last = Math.min((page + 1) * pageSize, filtered.length);
  $('#directory-page-status').textContent = `${first.toLocaleString('en')}–${last.toLocaleString('en')} of ${filtered.length.toLocaleString('en')} real institutions`;
  $('#directory-prev').disabled = page === 0;
  $('#directory-next').disabled = page >= pageCount - 1;
}

function setLoadFailure(error) {
  $('#directory-status').textContent = `Real score snapshot unavailable: ${error.message}`;
  $('#scope-notice').textContent = 'No score is shown until a versioned real-data snapshot is available.';
  $('#directory-rows').innerHTML = '<tr><td class="empty-state" colspan="6">The local real score snapshot is unavailable. Run the real-data ingest command first.</td></tr>';
  $('#source-status').textContent = 'No score data loaded. Synthetic/demo scores are never used as a fallback.';
}

async function loadRanking() {
  try {
    const snapshotUrl = `${import.meta.env.BASE_URL}data/local/us-academia-network.json`;
    const response = await fetch(snapshotUrl, { cache: 'no-store' });
    if (!response.ok) throw new Error(`snapshot request returned HTTP ${response.status}`);
    const bytes = await response.arrayBuffer();
    const manifestResponse = await fetch(`${snapshotUrl}.manifest.json`, { cache: 'no-store' });
    if (!manifestResponse.ok) throw new Error(`snapshot manifest request returned HTTP ${manifestResponse.status}`);
    const manifest = await manifestResponse.json();
    await verifySnapshotBytes(bytes, manifest);
    snapshot = validateScoreSnapshot(JSON.parse(new TextDecoder().decode(bytes)));
    if (snapshot.snapshot_revision !== manifest.snapshot_revision) throw new Error(`Snapshot ${snapshot.snapshot_revision} manifest mismatch: snapshot_revision`);
    records = snapshot.nodes;
    $('#node-count').textContent = snapshot.algorithm.nodes.toLocaleString('en');
    $('#edge-count').textContent = snapshot.algorithm.edges.toLocaleString('en');
    $('#scope-notice').textContent = `US employing institutions, ${snapshot.scope.employment_sample_start}–${snapshot.scope.employment_sample_end}; doctorate only. The score uses ${snapshot.algorithm.within_ranked_network_edges.toLocaleString('en')} internal network edges. ${snapshot.algorithm.external_doctoral_origin_edges.toLocaleString('en')} external doctoral-origin edges (${snapshot.algorithm.external_doctoral_origin_count.toLocaleString('en')} source records) are excluded from PageRank.`;
    $('#directory-status').textContent = `${snapshot.algorithm.nodes.toLocaleString('en')} real institutions · ${snapshot.algorithm.edges.toLocaleString('en')} observed directed relations · PageRank converged in ${snapshot.algorithm.iterations} iterations.`;
    const inputDigests = Object.entries(snapshot.input_sha256).map(([name, digest]) => `${name} ${digest}`).join(' · ');
    $('#source-status').textContent = `Revision ${snapshot.snapshot_revision} · pinned source commit ${snapshot.source.commit.slice(0, 12)} · input SHA-256 ${inputDigests} · ${snapshot.publication_status}.`;
    const boundary = snapshot.boundary_sensitivity;
    const comparison = boundary.rank_comparison_among_employer_nodes;
    $('#boundary-status').textContent = `Boundary sensitivity: Top 10 overlap ${comparison.top_10.overlap_count}/10, Top 25 ${comparison.top_25.overlap_count}/25, Top 50 ${comparison.top_50.overlap_count}/50 after adding ${boundary.all_observed_origins_as_nodes.external_origin_nodes.toLocaleString('en')} external doctoral-origin nodes. Employer rank shifts: median ${comparison.rank_shift_abs_median}, 95th percentile ${comparison.rank_shift_abs_p95}, maximum ${comparison.rank_shift_abs_max}. Raw scores are not directly comparable across node universes.`;
    renderDirectory();
  } catch (error) {
    setLoadFailure(error);
  }
}

for (const event of ['input', 'change']) {
  $('#directory-search').addEventListener(event, () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => { page = 0; renderDirectory(); }, 80);
  });
}
$('#directory-role').addEventListener('change', () => { page = 0; renderDirectory(); });
$('#directory-sort').addEventListener('change', () => { page = 0; renderDirectory(); });
$('#directory-prev').addEventListener('click', () => { page = Math.max(0, page - 1); renderDirectory(); });
$('#directory-next').addEventListener('click', () => { page += 1; renderDirectory(); });

loadRanking();
