"""Refresh licensed Chinese delivery subsets from the existing official provider.

No packages are installed. Only final WOFF2 assets and provenance are retained;
CSS responses stay in memory. Run explicitly with --download after adding text.
"""
import argparse
import hashlib
import json
import re
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CJK = re.compile(r'[\u3000-\u303f\u3400-\u9fff\uf900-\ufaff\uff01-\uff60]')


def corpus():
    paths = [ROOT/'index.html', ROOT/'vite.config.js']
    paths += sorted(p for p in (ROOT/'web').glob('*.js') if not p.name.endswith('.test.js'))
    text = ''.join(p.read_text(encoding='utf-8') for p in paths)
    # Public institutional names can require more glyphs than the UI itself.
    manifest = json.loads((ROOT/'outputs/easy-orcid-web/manifest.json').read_text())
    node_path = ROOT/'outputs/easy-orcid-web'/manifest['nodes']['file']
    raw = node_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest['nodes']['sha256']:
        raise ValueError('institution font corpus hash mismatch')
    text += json.dumps(json.loads(raw), ensure_ascii=False)
    current=json.loads((ROOT/'outputs/global/manifest.json').read_text())
    current_raw=(ROOT/'outputs/global'/current['snapshot']).read_bytes()
    if hashlib.sha256(current_raw).hexdigest()!=current['output_sha256']:
        raise ValueError('current observation font corpus hash mismatch')
    text += json.dumps(json.loads(current_raw),ensure_ascii=False)
    return ''.join(sorted(set(CJK.findall(text))))


def fetch(url, bound, host):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname != host or parsed.username or parsed.password or parsed.port:
        raise ValueError('unexpected font provider URL')
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):return None
    # Google Fonts negotiates TTF for generic clients and WOFF2 for browsers.
    request = urllib.request.Request(url, headers={'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
        raw = response.read(bound+1)
    if len(raw)>bound:raise ValueError('font response exceeds bound')
    return raw


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--download', action='store_true', required=True)
    parser.parse_args()
    characters=corpus()
    if not 0<len(characters)<=8192:raise ValueError('unexpected font corpus size')
    folder=ROOT/'web/fonts';entries=[];rules=[];total=0
    for i,start in enumerate(range(0,len(characters),384)):
        subset=characters[start:start+384]
        css_url='https://fonts.googleapis.com/css2?'+urllib.parse.urlencode({
            'family':'Noto Serif SC:wght@400','display':'swap','text':subset})
        css=fetch(css_url,65536,'fonts.googleapis.com')
        urls=re.findall(r'url\((https://[^)]+)\)',css.decode())
        if len(set(urls))!=1:raise ValueError('expected one official delivery subset')
        asset=fetch(urls[0],1024**2,'fonts.gstatic.com')
        if not asset.startswith(b'wOF2'):raise ValueError('provider did not return WOFF2')
        total+=len(asset)
        if total>2*1024**2:raise ValueError('font delivery exceeds 2 MiB')
        filename=f'song-cjk-{i}.woff2';target=folder/filename
        if target.exists() and target.read_bytes()!=asset:raise ValueError('inspect changed existing subset before replacement')
        if not target.exists():
            with target.open('xb') as out:out.write(asset)
        ranges=','.join(f'U+{ord(c):X}' for c in subset)
        for family,weight in (('GAPI Song','400'),('DM Sans','400 700'),('DM Mono','400')):
            rules.append("@font-face{font-family:'"+family+"';font-style:normal;font-weight:"+weight+";font-display:swap;src:url('./"+filename+"') format('woff2');unicode-range:"+ranges+"}")
        entries.append({'file':filename,'sha256':hashlib.sha256(asset).hexdigest(),'bytes':len(asset),
            'css_url':css_url,'css_sha256':hashlib.sha256(css).hexdigest(),'asset_url':urls[0],
            'codepoints':[ord(c) for c in subset]})
    coverage={'schema_version':'gapi-cjk-coverage/v1','license':'OFL-1.1',
        'upstream_family':'Noto Serif SC','weight':400,'characters':characters,
        'scope':'current UI source and both selected public observation corpora; not all world scripts',
        'assets':entries}
    (folder/'cjk-coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (folder/'cjk.css').write_text('\n'.join(rules)+'\n',encoding='utf-8')
    print(json.dumps({'glyphs':len(characters),'subsets':len(entries),'bytes':total}))


if __name__=='__main__':main()
