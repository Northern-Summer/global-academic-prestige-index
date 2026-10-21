import { defineConfig } from 'vite';
import { readFile } from 'node:fs/promises';

const base = process.env.GITHUB_ACTIONS === 'true' ? '/global-academic-prestige-index/' : '/';

const documentation = ['METHODOLOGY.md', 'DATA_SOURCES.md', 'DATA_SCHEMA.md', 'BIAS_AND_LIMITATIONS.md', 'RED_TEAM_REPORT.md'];

export default defineConfig({
  base,
  plugins: [{
    name: 'publish-method-docs',
    async generateBundle() {
      for (const filename of documentation) {
        const source = await readFile(new URL(`./docs/${filename}`, import.meta.url), 'utf8');
        this.emitFile({ type: 'asset', fileName: `docs/${filename}`, source });
      }
    },
  }],
});
