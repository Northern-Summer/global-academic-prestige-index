import { defineConfig } from 'vite';
import { readFile } from 'node:fs/promises';
import { createReadStream, existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, basename } from 'node:path';
import { validateGlobal } from './web/global-data.js';
import {DOCUMENTATION,sourceInputs,writeBuildProvenance,readReleaseSnapshot} from './scripts/build-provenance.mjs';
import {readDerivedBundle} from './scripts/derived-bundle.mjs';

const base = process.env.GITHUB_ACTIONS === 'true' ? '/global-academic-prestige-index/' : '/';

const documentation = DOCUMENTATION;
const buildRoot = dirname(fileURLToPath(import.meta.url));
const buildInputs = sourceInputs(buildRoot);

export default defineConfig({
  base,
  plugins: [{
    name: 'publish-method-docs',
    transformIndexHtml(html, context) {
      if (context.server) return html;
      // The US sample is local-only; production has no route or rights to it.
      return html.replace(/<!-- LOCAL_SAMPLE_START -->[\s\S]*?<!-- LOCAL_SAMPLE_END -->/g, '')
        .replace('<a href="#rankings">US sample</a>', '')
        .replace(/<aside class="intro-note">[\s\S]*?<\/aside>/, '<aside class="intro-note"><span class="note-index">THE CURRENT EDITION</span><p>2023 年公开履历已完成全文件处理。持续采集的 ORCID、Wikidata 与官方奖项另列。</p><a href="./docs/STREAMING.md">数据来源与范围 ↘</a><div class="note-stamp" aria-hidden="true">GAPI</div></aside>');
    },
    configureServer(server) {
      const localScores = fileURLToPath(new URL('./data/real/us-academia-network.json', import.meta.url));
      const scoreRoutes = new Map([
        ['/data/local/us-academia-network.json', localScores],
        ['/data/local/us-academia-network.json.manifest.json', `${localScores}.manifest.json`],
      ]);
      const taskConfig = JSON.parse(requireTaskConfig());
      const globalScores = resolve(taskConfig.work_dir, 'global-local.json');
      scoreRoutes.set('/data/global/manifest.json', `${globalScores}.manifest.json`);
      scoreRoutes.set('/data/global/global-local.json', globalScores);
      const derivedRoot=resolve(buildRoot,'outputs/easy-orcid-web');
      server.middlewares.use((request, response, next) => {
        const requestPath=request.url?.split('?')[0];
        const historical=/^\/data\/derived\/(manifest|[a-f0-9]{64})\.json$/.exec(requestPath||'');
        const filePath = historical?resolve(derivedRoot,`${historical[1]}.json`):scoreRoutes.get(requestPath);
        if (!filePath) return next();
        if (request.method !== 'GET' && request.method !== 'HEAD') return next();
        if (!existsSync(filePath)) {
          response.statusCode = 404;
          response.end('Local real score snapshot is unavailable.');
          return;
        }
        response.setHeader('Content-Type', 'application/json; charset=utf-8');
        response.setHeader('Cache-Control', 'no-store');
        if (request.method === 'HEAD') return response.end();
        createReadStream(filePath).on('error', next).pipe(response);
      });
    },
    async generateBundle() {
      for (const filename of documentation) {
        const source = await readFile(new URL(`./docs/${filename}`, import.meta.url), 'utf8');
        this.emitFile({ type: 'asset', fileName: `docs/${filename}`, source });
      }
      if (process.env.GAPI_CODE_ONLY === '1') return;
      const manifestPath = resolve(process.env.GAPI_RELEASE_MANIFEST || 'outputs/global/manifest.json');
      if (!existsSync(manifestPath)) throw new Error('A selected real release manifest is required. GAPI_CODE_ONLY=1 builds code/docs without a ranking.');
      const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
      const source = readReleaseSnapshot(manifestPath,manifest).bytes;
      const snapshot = validateGlobal(JSON.parse(source));
      const localOnly = process.env.GAPI_LOCAL_BUILD === '1';
      if (!localOnly && (manifest.publication_status !== 'public-observations-pending-external-review' || snapshot.publication_status !== manifest.publication_status)) throw new Error('Local/restricted dataset cannot be published');
      this.emitFile({type:'asset', fileName:`data/global/${basename(manifest.snapshot)}`, source});
      this.emitFile({type:'asset', fileName:'data/global/manifest.json', source:JSON.stringify(manifest)});
      const historical=readDerivedBundle(buildRoot,manifest.derived_bundle);
      if(historical)for(const [name,bytes] of historical.files)this.emitFile({type:'asset',fileName:`data/derived/${name}`,source:bytes});
    },
    writeBundle(options) {
      writeBuildProvenance({root:buildRoot,outDir:resolve(options.dir || 'dist'),base,expectedInputs:buildInputs,
        mode:process.env.GAPI_CODE_ONLY === '1' ? 'code-only' : process.env.GAPI_LOCAL_BUILD === '1' ? 'local-only' : 'public-observations',
        manifestPath:process.env.GAPI_RELEASE_MANIFEST || 'outputs/global/manifest.json'});
    },
  }],
});

function requireTaskConfig() {
  // A task-owned config supplies location; never enumerate arbitrary paths from requests.
  return readFileSync(new URL('./configs/streaming.json', import.meta.url), 'utf8');
}
