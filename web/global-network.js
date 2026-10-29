import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

export function showNetwork(container, slice, onPick) {
  let renderer;
  try { renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true }); }
  catch { container.textContent = '3D 暂不可用；请使用下方可分页的有向关系列表。'; return () => {}; }
  const width = Math.max(240, container.clientWidth), height = 360;
  renderer.setSize(width, height); renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
  renderer.domElement.setAttribute('aria-label', '3D employer to training network. Equivalent directional edges are listed below.');
  renderer.domElement.setAttribute('role', 'img');
  container.replaceChildren(renderer.domElement);
  const scene = new THREE.Scene(), camera = new THREE.PerspectiveCamera(45, width/height, 0.1, 1000);
  camera.position.set(0, 0, 110);
  const controls = new OrbitControls(camera, renderer.domElement); controls.enableDamping = false;
  const positions = new Map(), meshes = [];
  const colors = {Asia: '#b56842', Europe: '#497286', Africa: '#8a7046', Oceania: '#866a9e', 'North America': '#3b8a74', 'South America': '#9b637b'};
  const max = Math.max(0.000001, ...slice.nodes.map(n => n.network_score ?? 0));
  const geo = new THREE.SphereGeometry(1, 10, 8);
  for (const [i,node] of slice.nodes.entries()) {
    // Deterministic sphere layout: position has no geographical or causal meaning.
    const y = slice.nodes.length > 1 ? 1 - 2*i/(slice.nodes.length-1) : 0;
    const radius = Math.sqrt(Math.max(0,1-y*y)), theta = i * Math.PI*(3-Math.sqrt(5));
    const pos = new THREE.Vector3(Math.cos(theta)*radius*32, y*32, Math.sin(theta)*radius*32);
    positions.set(node.institution_id,pos);
    const mesh = new THREE.Mesh(geo,new THREE.MeshBasicMaterial({color:colors[node.region] ?? '#777777'}));
    mesh.position.copy(pos); mesh.scale.setScalar(.4 + 1.6*Math.sqrt((node.network_score ?? 0)/max));
    mesh.userData.institution = node.institution_id; scene.add(mesh); meshes.push(mesh);
  }
  for (const edge of slice.edges) {
    const from = positions.get(edge.source), to = positions.get(edge.target), delta = to.clone().sub(from);
    const arrow = new THREE.ArrowHelper(delta.clone().normalize(),from,delta.length(),0x81958f,1.1,.45);
    arrow.line.material.transparent = true; arrow.line.material.opacity = .35; scene.add(arrow);
  }
  const render = () => renderer.render(scene,camera);
  controls.addEventListener('change',render);
  const click = event => {
    const rect = renderer.domElement.getBoundingClientRect();
    const pointer = new THREE.Vector2((event.clientX-rect.left)/rect.width*2-1,-((event.clientY-rect.top)/rect.height)*2+1);
    const ray = new THREE.Raycaster(); ray.setFromCamera(pointer,camera);
    const hit = ray.intersectObjects(meshes)[0]; if (hit) onPick(hit.object.userData.institution);
  };
  renderer.domElement.addEventListener('click',click);
  const resize = new ResizeObserver(() => { const w=Math.max(240,container.clientWidth); renderer.setSize(w,height); camera.aspect=w/height; camera.updateProjectionMatrix(); render(); });
  resize.observe(container); render();
  return () => {
    resize.disconnect(); controls.dispose(); renderer.domElement.removeEventListener('click',click);
    scene.traverse(o => {o.geometry?.dispose(); if (Array.isArray(o.material)) o.material.forEach(m=>m.dispose()); else o.material?.dispose();});
    renderer.dispose(); renderer.forceContextLoss(); container.replaceChildren();
  };
}
