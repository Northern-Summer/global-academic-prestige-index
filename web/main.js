import * as THREE from 'three';
import data from '../data/demo/rankings.json';
import './style.css';

const $ = (selector) => document.querySelector(selector);
const esc = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);

const nodesById = new Map(data.nodes.map((node) => [node.institution_id, node]));

$('#institution-count').textContent = data.nodes.length;
$('#link-count').textContent = data.edges.length;
$('#ranked-count').textContent = data.nodes.filter((node) => node.status === 'ranked').length;
for (const region of [...new Set(data.nodes.map((node) => node.region))].sort()) {
  const option = document.createElement('option');
  option.value = region; option.textContent = region;
  $('#region-filter').append(option);
}

function renderTable() {
  const query = $('#search').value.trim().toLocaleLowerCase();
  const region = $('#region-filter').value;
  const rows = data.nodes.filter((node) => (region === 'all' || node.region === region) && `${node.institution_name} ${node.country} ${node.region}`.toLocaleLowerCase().includes(query));
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
  $('#ranking-rows').innerHTML = rows.map((node) => `<tr data-id="${esc(node.institution_id)}"><td>${node.rank ?? '—'}</td><td><button class="institution-select" type="button" data-id="${esc(node.institution_id)}">${esc(node.institution_name)}</button></td><td>${esc(node.region)}</td><td class="score">${node.network_score === null ? '—' : node.network_score.toFixed(4)}</td><td class="score">${node.award_score === null ? '—' : node.award_score.toFixed(2)}</td><td class="score">${node.composite_score === null ? '—' : node.composite_score.toFixed(3)}</td><td><span class="status ${node.status === 'ranked' ? 'ranked' : ''}">${node.status === 'ranked' ? `${node.coverage.event_count} events` : 'LOW COVERAGE'}</span></td></tr>`).join('');
  $('#ranking-rows').querySelectorAll('.institution-select').forEach((button) => button.addEventListener('click', () => selectNode(button.dataset.id)));
}
$('#search').addEventListener('input', renderTable);
$('#region-filter').addEventListener('change', renderTable);
$('#sort-filter').addEventListener('change', renderTable);
renderTable();

const container = $('#network-canvas');
let renderer;
let scene;
let camera;
let raycaster;
let pointer;
let nodeObjects = new Map();
let edgeObjects = [];
let hovered = null;
let selected = null;
const positions = new Map();
const nodeIds = [...nodesById.keys()];
nodeIds.forEach((id, index) => {
  const angle = (Math.PI * 2 * index) / nodeIds.length - Math.PI / 2;
  positions.set(id, new THREE.Vector3(Math.cos(angle) * 3.2, (index % 2 ? 0.65 : -0.45), Math.sin(angle) * 2.4));
});

function createLabel(text) {
  const canvas = document.createElement('canvas');
  canvas.width = 512; canvas.height = 88;
  const context = canvas.getContext('2d');
  context.fillStyle = '#e6f3ed'; context.font = '600 25px DM Sans, Arial'; context.textAlign = 'center'; context.fillText(text, 256, 52, 490);
  const texture = new THREE.CanvasTexture(canvas);
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: texture, transparent: true, depthWrite: false }));
  sprite.scale.set(2.15, 0.38, 1);
  return sprite;
}

function initializeScene() {
  try {
    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  } catch {
    container.innerHTML = '<p class="webgl-fallback">3D rendering is unavailable. Use the institution table and relationship list.</p>';
    return;
  }
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.setSize(container.clientWidth, container.clientHeight);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  container.appendChild(renderer.domElement);
  scene = new THREE.Scene();
  scene.background = new THREE.Color('#172a28');
  camera = new THREE.PerspectiveCamera(42, container.clientWidth / container.clientHeight, 0.1, 100);
  camera.position.set(0, 3.2, 8.7);
  scene.add(new THREE.HemisphereLight(0xd7fff0, 0x1b3330, 2.1));
  const light = new THREE.DirectionalLight(0xffffff, 1.2); light.position.set(-3, 5, 5); scene.add(light);
  raycaster = new THREE.Raycaster(); pointer = new THREE.Vector2();

  for (const node of data.nodes) {
    const position = positions.get(node.institution_id);
    const geometry = new THREE.SphereGeometry(node.status === 'ranked' ? 0.24 : 0.16, 22, 16);
    const material = new THREE.MeshStandardMaterial({ color: node.status === 'ranked' ? '#50aa94' : '#ce7966', roughness: 0.45, metalness: 0.08 });
    const mesh = new THREE.Mesh(geometry, material);
    mesh.position.copy(position); mesh.userData.id = node.institution_id; scene.add(mesh); nodeObjects.set(node.institution_id, mesh);
    const label = createLabel(node.institution_name.replace(' (DEMO)', ''));
    label.position.copy(position).add(new THREE.Vector3(0, 0.45, 0)); scene.add(label);
  }
  edgeObjects = data.edges.map((edge) => {
    const origin = positions.get(edge.source); const destination = positions.get(edge.target);
    const direction = destination.clone().sub(origin).normalize();
    const start = origin.clone().addScaledVector(direction, 0.32);
    const end = destination.clone().addScaledVector(direction, -0.35);
    const line = new THREE.Line(new THREE.BufferGeometry().setFromPoints([start, end]), new THREE.LineBasicMaterial({ color: '#df9276', transparent: true, opacity: 0.7 }));
    scene.add(line);
    const arrow = new THREE.ConeGeometry(0.075, 0.22, 10);
    const head = new THREE.Mesh(arrow, new THREE.MeshBasicMaterial({ color: '#ed9a7b' }));
    head.position.copy(end); head.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), direction); scene.add(head);
    return { edge, line, head };
  });

  let dragging = false; let previous = { x: 0, y: 0 }; let rotation = { x: 0, y: 0 };
  renderer.domElement.addEventListener('pointerdown', (event) => { dragging = true; previous = { x: event.clientX, y: event.clientY }; renderer.domElement.setPointerCapture(event.pointerId); });
  renderer.domElement.addEventListener('pointerup', (event) => { dragging = false; renderer.domElement.releasePointerCapture(event.pointerId); });
  renderer.domElement.addEventListener('pointermove', (event) => {
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    if (dragging) {
      rotation.y += (event.clientX - previous.x) * 0.007;
      rotation.x = Math.max(-0.65, Math.min(0.65, rotation.x + (event.clientY - previous.y) * 0.004));
      previous = { x: event.clientX, y: event.clientY };
    }
    raycaster.setFromCamera(pointer, camera);
    const hit = raycaster.intersectObjects([...nodeObjects.values()])[0]?.object ?? null;
    hovered = hit?.userData.id ?? null;
    renderer.domElement.style.cursor = hit ? 'pointer' : dragging ? 'grabbing' : 'grab';
    if (hit && !dragging) selectNode(hit.userData.id);
  });
  renderer.domElement.addEventListener('click', () => { if (hovered) selectNode(hovered); });
  renderer.domElement.addEventListener('wheel', (event) => { camera.position.z = Math.max(5, Math.min(13, camera.position.z + event.deltaY * 0.008)); }, { passive: true });
  const resize = new ResizeObserver(() => {
    if (!renderer || !container.clientWidth || !container.clientHeight) return;
    renderer.setSize(container.clientWidth, container.clientHeight);
    camera.aspect = container.clientWidth / container.clientHeight; camera.updateProjectionMatrix();
  });
  resize.observe(container);
  const animate = () => {
    requestAnimationFrame(animate);
    scene.rotation.y += (rotation.y - scene.rotation.y) * 0.08;
    scene.rotation.x += (rotation.x - scene.rotation.x) * 0.08;
    for (const [id, object] of nodeObjects) {
      const active = id === selected || id === hovered;
      const scale = active ? 1.22 : 1;
      object.scale.setScalar(scale);
    }
    updateEdges();
    renderer.render(scene, camera);
  };
  animate();
  updateEdges();
}

function updateEdges() {
  if (!edgeObjects.length) return;
  const minFlow = Number($('#flow-filter').value);
  const stage = $('#stage-filter').value;
  $('#flow-value').textContent = minFlow.toFixed(1);
  for (const { edge, line, head } of edgeObjects) {
    const weight = stage === 'all' ? edge.weight : (edge.stages[stage] ?? 0);
    const visible = weight >= minFlow && weight > 0;
    line.visible = visible; head.visible = visible;
    line.material.opacity = Math.min(0.84, 0.26 + weight * 0.3);
  }
}

function selectNode(id) {
  if (!id || !nodesById.has(id)) return;
  selected = id;
  const node = nodesById.get(id);
  $('#detail-name').textContent = node.institution_name;
  const incoming = data.edges.filter((edge) => edge.target === id).sort((a, b) => b.weight - a.weight);
  const outgoing = data.edges.filter((edge) => edge.source === id).sort((a, b) => b.weight - a.weight);
  $('#detail-copy').textContent = `${node.status === 'ranked' ? `Demo rank ${node.rank}. ` : 'Not ranked in demo. '}Observed links point from hiring institutions to training institutions.`;
  $('#detail-awards').textContent = node.award_categories ? `Award components: ${Object.entries(node.award_categories).map(([category, score]) => `${category.replace('_', ' ')} ${score.toFixed(2)}`).join(' · ') || 'none'}` : 'Award components withheld: institution is not eligible for ranking.';
  $('#detail-links').innerHTML = [...incoming.slice(0, 3).map((edge) => ({ label: `Hired by ${nodesById.get(edge.source).institution_name}`, weight: edge.weight })), ...outgoing.slice(0, 2).map((edge) => ({ label: `Trained talent hired at ${nodesById.get(edge.target).institution_name}`, weight: edge.weight }))].map((item) => `<li>${esc(item.label)} <span>${item.weight.toFixed(2)}</span></li>`).join('') || '<li>No observed demo links</li>';
}

$('#stage-filter').addEventListener('change', updateEdges);
$('#flow-filter').addEventListener('input', updateEdges);
initializeScene();
selectNode(data.nodes.find((node) => node.rank === 1)?.institution_id ?? data.nodes[0]?.institution_id);
