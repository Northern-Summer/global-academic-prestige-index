import { defineConfig } from 'vite';
import { readFile } from 'node:fs/promises';
import { createReadStream, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const base = process.env.GITHUB_ACTIONS === 'true' ? '/global-academic-prestige-index/' : '/';

const documentation = ['METHODOLOGY.md', 'DATA_SOURCES.md', 'DATA_SCHEMA.md', 'BIAS_AND_LIMITATIONS.md', 'RED_TEAM_REPORT.md'];

export default defineConfig({
  base,
  plugins: [{
    name: 'publish-method-docs',
    configureServer(server) {
      const localScores = fileURLToPath(new URL('./data/real/us-academia-network.json', import.meta.url));
      const scoreRoutes = new Map([
        ['/data/local/us-academia-network.json', localScores],
        ['/data/local/us-academia-network.json.manifest.json', `${localScores}.manifest.json`],
      ]);
      server.middlewares.use((request, response, next) => {
        const filePath = scoreRoutes.get(request.url?.split('?')[0]);
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
    },
  }],
});
