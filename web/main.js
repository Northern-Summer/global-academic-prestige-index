import * as THREE from 'three';
import data from '../data/demo/rankings.json';
import scenarioData from '../data/demo/scenarios.json';
import { edgeWeightRange, edgesForInstitution, filterEdges, nodeColor } from './state.js';
import './style.css';

const $ = (selector) => document.querySelector(selector);
const esc = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);

document.documentElement.style.setProperty('--gapi-node-ranked', nodeColor('ranked'));
document.documentElement.style.setProperty('--gapi-node-unranked', nodeColor('unranked'));

const nodesById = new Map(data.nodes.map((node) => [node.institution_id, node]));
let currentScenario = scenarioData.scenarios[0];
let currentNodes = currentScenario.result.nodes;
let currentEdges = currentScenario.result.edges;
let selected = null;
let edgeObjects = [];
const nodeObjects = new Map();
const labelObjects = new Map();
const positions = new Map();
const nodesByCurrentId = () => new Map(currentNodes.map((node) => [node.institution_id, node]));

$('#scenario-filter').innerHTML = scenarioData.scenarios.map((scenario) => `<option value="${esc(scenario.id)}">${esc(scenario.label)}</option>`).join('');
for (const region of [...new Set(data.nodes.map((node) => node.region))].sort()) {
  const option = document.createElement('option'); option.value = region; option.textContent = region; $('#region-filter').append(option);
}

function filteredEdges() {
  const stage = $('#stage-filter').value;
  const minFlow = Number($('#flow-filter').value);
  return filterEdges(currentEdges, { stage, minWeight: minFlow });
}

function renderTable() {
  const query = $('#search').value.trim().toLocaleLowerCase();
  const region = $('#region-filter').value;
  const currentById = nodesByCurrentId();
  const rows = currentNodes.filter((node) => (region === 'all' || node.region === region) && `${node.institution_name} ${node.country} ${node.region}`.toLocaleLowerCase().includes(query));
  const sort = $('#sort-filter').value;
  rows.sort((a, b) => {
    if (sort === 'name') return a.institution_name.localeCompare(b.institution_name);
    if (sort === 'network' || sort === 'awards') {
      const key = sort === 'network' ? 'network_score' : 'award_score';
      return (b[key] ?? -1) - (a[key] ?? -1) || a.institution_name.localeCompare(b.institution_name);
    }
    if (a.rank === null && b.rank !== null) return 1;
    if (b.rank === null && a.rank !== null) return -1;
    return (a.rank ?? 999999) - (b.rank ?? 999999) || a.institution_name.localeCompare(b.institution_name);
  });
  $('#ranking-rows').innerHTML = rows.map((node) => `<tr data-id="${esc(node.institution_id)}" class="${node.institution_id === selected ? 'is-selected' : ''}"><td class="rank-cell">${node.rank === null ? '<span class="unranked-rank">—</span>' : `<span class="rank-pill">${node.rank}</span>${currentNodes.filter((item) => item.rank === node.rank).length > 1 ? '<span class="tie-note">TIE</span>' : ''}`}</td><td><button class="institution-select" type="button" data-id="${esc(node.institution_id)}">${esc(node.institution_name)}<small>${esc(node.institution_type.replaceAll('_', ' '))}</small></button></td><td>${esc(node.region)}</td><td class="score">${node.network_score === null ? '—' : node.network_score.toFixed(4)}</td><td class="score">${node.award_score === null ? '—' : node.award_score.toFixed(2)}</td><td class="score composite-score">${node.composite_score === null ? '—' : node.composite_score.toFixed(3)}</td><td>${node.status === 'ranked' ? `<span class="status ranked">${node.coverage.event_count} events</span>` : `<span class="status unranked" title="${esc(node.unranked_reasons.join(', '))}">Not ranked · ${node.coverage.event_count} events</span>`}</td></tr>`).join('') || '<tr><td class="empty-state" colspan="7">No institutions match these filters.</td></tr>';
  $('#ranking-rows').querySelectorAll('.institution-select').forEach((button) => button.addEventListener('click', () => selectNode(button.dataset.id)));
  $('#ranked-count').textContent = String(currentNodes.filter((node) => node.status === 'ranked').length);
}

function updateSliderRange() {
  const range = edgeWeightRange(currentEdges, $('#stage-filter').value);
  const slider = $('#flow-filter');
  const { min, max } = range;
  const step = max <= 1 ? 0.1 : Math.max(0.1, Math.round(max / 20 * 10) / 10);
  slider.min = '0'; slider.max = String(Math.max(0, max)); slider.step = String(step);
  if (Number(slider.value) > max) slider.value = String(min);
  $('#flow-range-note').textContent = max ? `Observed range ${min.toFixed(1)}–${max.toFixed(1)} · ${range.count} links` : 'No flow in this stage';
}

function updateMetrics() {
  $('#institution-count').textContent = String(currentNodes.length);
  $('#link-count').textContent = String(currentEdges.length);
  $('#visible-edge-count').textContent = String(filteredEdges().length);
  $('#selected-network-label').textContent = currentScenario.label;
}

function renderNodePicker() {
  $('#node-picker').innerHTML = currentNodes.map((node) => `<button type="button" data-id="${esc(node.institution_id)}" class="node-chip ${node.institution_id === selected ? 'is-selected' : ''}" aria-pressed="${node.institution_id === selected}"><span>${node.rank === null ? '—' : String(node.rank).padStart(2, '0')}</span>${esc(node.institution_name.replace(' (DEMO)', ''))}</button>`).join('');
  $('#node-picker').querySelectorAll('button').forEach((button) => button.addEventListener('click', () => selectNode(button.dataset.id)));
}

function renderEdgeList(edges) {
  const currentById = nodesByCurrentId();
  const directed = edgesForInstitution(edges, selected);
  const incoming = directed.incoming.sort((a, b) => b.visible_weight - a.visible_weight);
  const outgoing = directed.outgoing.sort((a, b) => b.visible_weight - a.visible_weight);
  const items = [...incoming.map((edge) => ({ label: `Hired by ${currentById.get(edge.source)?.institution_name ?? 'unknown institution'}`, edge, direction: 'in' })), ...outgoing.map((edge) => ({ label: `Trained talent hired at ${currentById.get(edge.target)?.institution_name ?? 'unknown institution'}`, edge, direction: 'out' }))];
  $('#detail-links').innerHTML = items.map(({ label, edge, direction }) => `<li><span class="edge-direction ${direction}" aria-label="${direction === 'in' ? 'Incoming' : 'Outgoing'}">${direction === 'in' ? '←' : '→'}</span><span>${esc(label)}<small>${esc($('#stage-filter').value === 'all' ? Object.keys(edge.stages).join(', ') : $('#stage-filter').value)}</small></span><strong>${edge.visible_weight.toFixed(2)}</strong></li>`).join('') || '<li class="no-links">No visible links for this selection. Adjust the stage or flow threshold.</li>';
}

function updateEdges() {
  const visibleEdges = filteredEdges();
  const visibleKeys = new Set(visibleEdges.map((edge) => `${edge.source}|${edge.target}`));
  edgeObjects.forEach(({ edge, line, head }) => {
    const visible = visibleKeys.has(`${edge.source}|${edge.target}`);
    line.visible = visible; head.visible = visible;
    if (visible) {
      const edgeData = visibleEdges.find((item) => item.source === edge.source && item.target === edge.target);
      line.material.opacity = Math.min(0.9, 0.35 + (edgeData?.visible_weight ?? 0) * 0.045);
    }
  });
  $('#flow-value').textContent = Number($('#flow-filter').value).toFixed(1);
  $('#visible-edge-count').textContent = String(visibleEdges.length);
  if (selected) renderEdgeList(visibleEdges);
}

function selectNode(id) {
  const currentById = nodesByCurrentId();
  if (!id || !currentById.has(id)) return;
  selected = id;
  const node = currentById.get(id);
  $('#detail-name').textContent = node.institution_name;
  $('#detail-index').textContent = `RANK / ${node.rank === null ? '—' : String(node.rank).padStart(2, '0')}`;
  $('#selected-network-label').textContent = node.institution_name.replace(' (DEMO)', '');
  $('#detail-copy').textContent = `${node.status === 'ranked' ? `Scenario position ${node.rank}${currentNodes.filter((item) => item.rank === node.rank).length > 1 ? ' (tied)' : ''}. ` : 'Not ranked in this scenario. '}${node.coverage.event_count} constructed events are visible in this fixture. This is not real-world evidence.`;
  $('#detail-awards').textContent = node.award_categories ? `Separate award signal · ${Object.entries(node.award_categories).map(([category, score]) => `${category.replaceAll('_', ' ')} ${score.toFixed(2)}`).join(' · ') || 'none in this fixture'}` : `Not-ranked reasons · ${(node.unranked_reasons ?? []).map((reason) => reason.replaceAll('_', ' ')).join(' · ') || 'not eligible'}`;
  renderEdgeList(filteredEdges()); renderNodePicker(); renderTable();
}

function applyScenario(scenarioId) {
  currentScenario = scenarioData.scenarios.find((scenario) => scenario.id === scenarioId) ?? scenarioData.scenarios[0];
  currentNodes = currentScenario.result.nodes;
  currentEdges = currentScenario.result.edges;
  selected = currentNodes.some((node) => node.institution_id === selected) ? selected : currentNodes[0]?.institution_id;
  const scenarioNodes = nodesByCurrentId();
  for (const [id, object] of nodeObjects) {
    const node = scenarioNodes.get(id);
    object.visible = Boolean(node);
    const label = labelObjects.get(id);
    if (label) label.visible = Boolean(node);
    if (node) object.material.color.set(nodeColor(node.status));
  }
  $('#scenario-explainer').innerHTML = `<span class="scenario-kicker">${esc(currentScenario.label.toUpperCase())}</span><span>${esc(currentScenario.description)}</span><span class="scenario-disclaimer">Synthetic teaching scenario</span>`;
  updateSliderRange(); renderTable(); updateMetrics(); renderNodePicker(); selectNode(selected);
  edgeObjects.forEach(({ line, head }) => { line.visible = false; head.visible = false; });
  if (scene) rebuildEdges();
}

$('#search').addEventListener('input', renderTable);
$('#region-filter').addEventListener('change', renderTable);
$('#sort-filter').addEventListener('change', renderTable);
$('#scenario-filter').addEventListener('change', (event) => applyScenario(event.target.value));
$('#stage-filter').addEventListener('change', () => { updateSliderRange(); updateEdges(); renderEdgeList(filteredEdges()); });
$('#flow-filter').addEventListener('input', updateEdges);

const container = $('#network-canvas');
let renderer;
let scene;
let camera;
let raycaster;
let pointer;
let hovered = null;
let dragging = false;
let previous = { x: 0, y: 0 };
let rotation = { x: 0, y: 0 };
const nodeIds = [...nodesById.keys()];
nodeIds.forEach((id, index) => {
  const angle = (Math.PI * 2 * index) / nodeIds.length - Math.PI / 2;
  positions.set(id, new THREE.Vector3(Math.cos(angle) * 3.2, (index % 2 ? 0.65 : -0.45), Math.sin(angle) * 2.4));
});

function createLabel(text) {
  const canvas = document.createElement('canvas'); canvas.width = 512; canvas.height = 88;
  const context = canvas.getContext('2d'); context.fillStyle = '#f7f3e9'; context.font = '600 25px Arial'; context.textAlign = 'center'; context.fillText(text, 256, 52, 490);
  const texture = new THREE.CanvasTexture(canvas);
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: texture, transparent: true, depthWrite: false })); sprite.scale.set(2.15, 0.38, 1); return sprite;
}

function rebuildEdges() {
  const scenarioNodes = nodesByCurrentId();
  for (const { line, head } of edgeObjects) { scene.remove(line, head); line.geometry.dispose(); line.material.dispose(); head.geometry.dispose(); head.material.dispose(); }
  edgeObjects = currentEdges.map((edge) => {
    if (!scenarioNodes.has(edge.source) || !scenarioNodes.has(edge.target)) return null;
    const origin = positions.get(edge.source); const destination = positions.get(edge.target);
    if (!origin || !destination) return null;
    const direction = destination.clone().sub(origin).normalize();
    const start = origin.clone().addScaledVector(direction, 0.32); const end = destination.clone().addScaledVector(direction, -0.35);
    const line = new THREE.Line(new THREE.BufferGeometry().setFromPoints([start, end]), new THREE.LineBasicMaterial({ color: '#d7896b', transparent: true, opacity: 0.7 })); scene.add(line);
    const head = new THREE.Mesh(new THREE.ConeGeometry(0.075, 0.22, 10), new THREE.MeshBasicMaterial({ color: '#e0a081' })); head.position.copy(end); head.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), direction); scene.add(head);
    return { edge, line, head };
  }).filter(Boolean);
  updateEdges();
}

function initializeScene() {
  try { renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true }); }
  catch { container.innerHTML = '<div class="webgl-fallback"><strong>3D view unavailable</strong><span>Use the institution list and relationship details to explore the same data.</span></div>'; return; }
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2)); renderer.setSize(container.clientWidth, container.clientHeight); renderer.outputColorSpace = THREE.SRGBColorSpace; container.appendChild(renderer.domElement);
  scene = new THREE.Scene(); scene.background = new THREE.Color('#18312f');
  camera = new THREE.PerspectiveCamera(42, container.clientWidth / container.clientHeight, 0.1, 100); camera.position.set(0, 3.2, 8.7);
  scene.add(new THREE.HemisphereLight(0xe2f2e9, 0x203c38, 2.1)); const light = new THREE.DirectionalLight(0xffffff, 1.2); light.position.set(-3, 5, 5); scene.add(light);
  raycaster = new THREE.Raycaster(); pointer = new THREE.Vector2();
  for (const node of data.nodes) {
    const position = positions.get(node.institution_id); const geometry = new THREE.SphereGeometry(0.2, 22, 16);
    const material = new THREE.MeshStandardMaterial({ color: nodeColor(node.status), roughness: 0.45, metalness: 0.08 });
    const mesh = new THREE.Mesh(geometry, material); mesh.position.copy(position); mesh.userData.id = node.institution_id; scene.add(mesh); nodeObjects.set(node.institution_id, mesh);
    const label = createLabel(node.institution_name.replace(' (DEMO)', '')); label.position.copy(position).add(new THREE.Vector3(0, 0.45, 0)); scene.add(label); labelObjects.set(node.institution_id, label);
  }
  rebuildEdges();
  renderer.domElement.addEventListener('pointerdown', (event) => { dragging = true; previous = { x: event.clientX, y: event.clientY }; renderer.domElement.setPointerCapture(event.pointerId); });
  renderer.domElement.addEventListener('pointerup', (event) => { dragging = false; if (renderer.domElement.hasPointerCapture(event.pointerId)) renderer.domElement.releasePointerCapture(event.pointerId); });
  renderer.domElement.addEventListener('pointermove', (event) => {
    const rect = renderer.domElement.getBoundingClientRect(); pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1; pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    if (dragging) { rotation.y += (event.clientX - previous.x) * 0.007; rotation.x = Math.max(-0.65, Math.min(0.65, rotation.x + (event.clientY - previous.y) * 0.004)); previous = { x: event.clientX, y: event.clientY }; }
    raycaster.setFromCamera(pointer, camera); const hit = raycaster.intersectObjects([...nodeObjects.values()].filter((object) => object.visible))[0]?.object ?? null; hovered = hit?.userData.id ?? null; renderer.domElement.style.cursor = hit ? 'pointer' : dragging ? 'grabbing' : 'grab';
  });
  renderer.domElement.addEventListener('click', () => { if (hovered) selectNode(hovered); });
  renderer.domElement.addEventListener('wheel', (event) => { camera.position.z = Math.max(5, Math.min(13, camera.position.z + event.deltaY * 0.008)); }, { passive: true });
  const resize = new ResizeObserver(() => { if (!renderer || !container.clientWidth || !container.clientHeight) return; renderer.setSize(container.clientWidth, container.clientHeight); camera.aspect = container.clientWidth / container.clientHeight; camera.updateProjectionMatrix(); }); resize.observe(container);
  const animate = () => { requestAnimationFrame(animate); scene.rotation.y += (rotation.y - scene.rotation.y) * 0.08; scene.rotation.x += (rotation.x - scene.rotation.x) * 0.08; for (const [id, object] of nodeObjects) object.scale.setScalar(id === selected || id === hovered ? 1.22 : 1); renderer.render(scene, camera); }; animate();
}

updateSliderRange(); renderTable(); updateMetrics(); initializeScene(); applyScenario(currentScenario.id);
