/* Run with the already installed Playwright path; never downloads a browser. */
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const {createHash}=require('node:crypto');
const { chromium } = require(process.env.GAPI_PLAYWRIGHT || 'playwright');
const screenshotPath=name=>`${process.env.GAPI_TEST_SCREENSHOT_PREFIX || 'outputs/review'}-${name}-2026-10-29.png`;
const receiptPath=process.env.GAPI_TEST_RECEIPT || 'outputs/browser-review-2026-10-29.json';

(async () => {
  const browser = await chromium.launch({headless:true, executablePath:process.env.GAPI_BROWSER, args:['--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  const results = {tested_url:process.env.GAPI_TEST_URL || 'http://127.0.0.1:5174/'}, errors = [], requests = [];
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const loadStarted=Date.now();
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => requests.push(request.url()));
    await page.goto(process.env.GAPI_TEST_URL || 'http://127.0.0.1:5174/', {waitUntil:'networkidle',timeout:30000});
    await page.waitForFunction(() => /版本已校验|真实数据暂不可用/.test(document.querySelector('#global-status')?.textContent || ''));
    const status = await page.locator('#global-status').innerText();
    assert.ok(!status.includes('真实数据暂不可用'),status);
    results.load = status;
    results.table_ready_ms=Date.now()-loadStarted;
    if(results.tested_url.startsWith('http://127.0.0.1'))assert.ok(results.table_ready_ms<10000,'local table readiness exceeds 10s acceptance');
    results.version = await page.evaluate(async () => {
      const base = new URL('.', location.href);
      const manifest = await (await fetch(new URL('data/global/manifest.json',base))).json();
      const response = await fetch(new URL('data/global/'+manifest.snapshot,base));
      const bytes = await response.arrayBuffer();
      const sha = [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
      const data = JSON.parse(new TextDecoder().decode(bytes));
      return {snapshot:manifest.snapshot,expected:manifest.output_sha256,actual:sha,institutions:data.nodes.length,events:data.coverage.events,edges:data.edges.length,people:data.coverage.people,relations:data.nodes.reduce((sum,n)=>sum+n.relations,0),scored:data.nodes.filter(n=>n.network_score!=null).length,award_recipients:data.official_award_inventory?.processed_recipients,award_attribution_labels:data.official_award_attribution?.label_count};
    });
    assert.equal(results.version.actual,results.version.expected);
    assert.match(status,/正式全球排名尚未发布/);
    const metricText=await page.locator('#global-metrics').innerText();
    for (const value of [results.version.people,results.version.events,results.version.relations,results.version.scored]) assert.ok(metricText.replaceAll(',','').includes(String(value)));
    results.coverage_counts_match=true;
    const acquisition=await page.evaluate(async()=>{
      const base=new URL('.',location.href),m=await(await fetch(new URL('data/global/manifest.json',base))).json();
      return(await(await fetch(new URL('data/global/'+m.snapshot,base))).json()).acquisition_progress;
    });
    if(acquisition?.sources?.length){
      await page.locator('#global-acquisition summary').click();
      const bars=page.locator('#global-acquisition progress');
      assert.equal(await bars.count(),acquisition.sources.length);
      for(let i=0;i<acquisition.sources.length;i++){
        const s=acquisition.sources[i];
        assert.equal(await bars.nth(i).getAttribute('value'),String(s.processed));
        assert.equal(await bars.nth(i).getAttribute('max'),String(s.candidates));
        assert.match(await bars.nth(i).getAttribute('aria-label'),/已发现候选/);
      }
      assert.match(await page.locator('#global-acquisition').innerText(),/不代表全球学术人员覆盖率/);
      await page.locator('#global-acquisition').screenshot({path:screenshotPath('acquisition')});
      await page.locator('#global-acquisition summary').click();
      results.acquisition_progress_matches_snapshot=true;
    }
    await page.evaluate(()=>document.fonts.ready);
    results.typography=await page.evaluate(()=>({
      hero_family:getComputedStyle(document.querySelector('#page-title')).fontFamily,
      hero_italic:getComputedStyle(document.querySelector('#page-title em')).fontStyle,
      heading_family:getComputedStyle(document.querySelector('#global-title')).fontFamily,
      heading_weight:getComputedStyle(document.querySelector('#global-title')).fontWeight,
      fonts:[...document.fonts].map(font=>({family:font.family,style:font.style,status:font.status}))
    }));
    assert.match(results.typography.hero_family,/Playfair Display/);
    assert.equal(results.typography.hero_italic,'italic');
    assert.match(results.typography.heading_family,/GAPI Song/);
    assert.equal(results.typography.heading_weight,'400');
    for(const [family,style]of [['Playfair Display','normal'],['Playfair Display','italic'],['GAPI Song','normal']])
      assert.ok(results.typography.fonts.some(font=>font.family.replaceAll('"','')===family&&font.style===style&&font.status==='loaded'),`bundled font did not load: ${family} ${style}`);
    assert.equal(requests.some(url=>/fonts\.(googleapis|gstatic)\.com/.test(url)),false);
    await page.evaluate(()=>window.scrollTo(0,0));
    await page.screenshot({path:screenshotPath('hero')});
    const typography=await page.locator('#global-rows td').first().evaluate(el=>parseFloat(getComputedStyle(el).fontSize));
    assert.ok(typography>=14);results.table_min_font_px=typography;
    if (process.env.GAPI_PUBLIC_TEST === '1') {
      assert.equal(await page.locator('#rankings').count(),0);
      assert.equal(requests.some(url=>url.includes('/data/local/')),false);
      results.public_source_isolation = true;
    }
    const initialRows = await page.locator('#global-rows tr').count();
    assert.equal(initialRows,50);
    await page.locator('#global-search').fill('Cambridge');
    await page.waitForTimeout(150);
    assert.ok((await page.locator('#global-rows').innerText()).includes('Cambridge'));
    await page.locator('#global-search').fill('');
    await page.locator('#global-country').selectOption('CN');
    assert.ok((await page.locator('#global-rows').innerText()).includes('中国'));
    await page.locator('#global-country').selectOption('');
    await page.locator('#global-sort').selectOption('name');
    await page.locator('#global-sort').selectOption('score');
    const pick = page.locator('#global-rows button').first();
    await pick.focus(); await page.keyboard.press('Enter');
    assert.match(await page.locator('#global-details').innerText(),/暂未正式排名/);
    assert.ok(await page.locator('#global-details .mini-bars').count()>=1);
    await page.locator('#global-details').screenshot({path:screenshotPath('detail')});
    results.search_filter_sort_keyboard_details = true;
    const downloadPromise = page.waitForEvent('download');
    await page.locator('#global-csv').click();
    const download = await downloadPromise;
    assert.equal(download.suggestedFilename(),'gapi-real-observed-institutions.csv');
    await download.cancel();
    results.csv_download = true;
    if(results.version.award_recipients){
      await page.locator('#global-official-awards summary').click();
      await page.locator('#global-award-search').fill('Cambridge');
      assert.match(await page.locator('#global-award-rows').innerText(),/Cambridge/);
      if(results.version.award_attribution_labels){
        assert.match(await page.locator('#global-award-content').innerText(),/单机构.*份额/);
        assert.match(await page.locator('#global-award-rows').innerText(),/待确认|待核验|待解析/);
        results.official_award_attribution=true;
      }
      await page.locator('#global-official-awards summary').click();
      results.official_awards=true;
    }
    await page.locator('#global-reset').click();
    if(await page.locator('#global-edge-next').isEnabled()){
      const first=await page.locator('#global-edges tr').first().innerText();
      await page.locator('#global-edge-next').click();
      assert.notEqual(await page.locator('#global-edges tr').first().innerText(),first);
      results.relationship_pagination=true;
    }
    await page.locator('#global-reset').click();
    await page.locator('#global-show-network').click();
    await page.waitForFunction(() => document.querySelector('#global-network canvas') || /暂不可用/.test(document.querySelector('#global-network')?.textContent || ''));
    results.webgl = await page.locator('#global-network canvas').count() === 1;
    if (results.webgl) {
      assert.equal(await page.locator('#global-network-view').inputValue(),'focus');
      const focusedStatus=await page.locator('#global-edge-status').innerText();
      assert.match(focusedStatus,/当前视图上限为 80 个节点 \/ 40 条边/);
      const counts=focusedStatus.match(/3D 显示 (\d+) 个节点、(\d+) 条边/);
      assert.ok(counts && Number(counts[1])<=80 && Number(counts[2])<=40);
      const waitForScene=async counts=>page.waitForFunction(([nodes,edges])=>{
        const canvas=document.querySelector('#global-network canvas');
        return canvas?.dataset.renderedNodes===nodes && canvas?.dataset.renderedEdges===edges;
      },counts);
      await waitForScene([counts[1],counts[2]]);
      results.network_focus_counts={nodes:Number(counts[1]),edges:Number(counts[2])};
      const canvas=page.locator('#global-network canvas');
      await canvas.scrollIntoViewIfNeeded();
      await page.locator('#global-network').screenshot({path:screenshotPath('network')});
      const box=await canvas.boundingBox();
      // First node is at (0,32,0) in the declared fixed sphere layout. This
      // exercises the real raycaster, text label and pointer-leave behavior.
      await page.mouse.move(box.x+box.width/2,box.y+box.height/2-(32/110)*(box.height/2/Math.tan(Math.PI/8)));
      await page.waitForFunction(()=>{const t=document.querySelector('.network-tooltip');return t&&!t.hidden&&t.textContent.length>0;});
      results.network_hover_label=await page.locator('.network-tooltip').innerText();
      await page.locator('#global-network').screenshot({path:screenshotPath('network-hover')});
      await page.mouse.move(0,0);
      await page.waitForFunction(()=>document.querySelector('.network-tooltip')?.hidden);
      const tableRows=await page.locator('#global-edges').innerText();
      await page.locator('#global-network-view').selectOption('all');
      const allStatus=await page.locator('#global-edge-status').innerText();
      assert.match(allStatus,/500 个节点 \/ 2,000 条边/);
      const allCounts=allStatus.match(/3D 显示 (\d+) 个节点、(\d+) 条边/);
      await waitForScene([allCounts[1],allCounts[2]]);
      results.network_all_counts={nodes:Number(allCounts[1]),edges:Number(allCounts[2])};
      assert.equal(await page.locator('#global-edges').innerText(),tableRows);
      await page.locator('#global-network-view').selectOption('focus');
      await waitForScene([counts[1],counts[2]]);
      await page.locator('#global-network-view').selectOption('all');
      await page.locator('#global-network-view').selectOption('focus');
      await waitForScene([counts[1],counts[2]]);
      assert.equal(await page.locator('#global-network canvas').count(),1);
      results.network_rapid_switch_settles_latest=true;
      results.network_focus_and_full_preserve_table=true;
    }
    await page.locator('#global-stage').selectOption('postdoc');
    results.direction = await page.locator('#global-edge-status').innerText();
    await page.locator('#global-stage').selectOption('all');
    await page.locator('#global-observations').scrollIntoViewIfNeeded();
    await page.evaluate(() => window.scrollTo(0,document.querySelector('#global-observations').offsetTop-12));
    await page.screenshot({path:screenshotPath('desktop')});
    await page.setViewportSize({width:390,height:844});
    await page.evaluate(()=>window.scrollTo(0,0));
    await page.screenshot({path:screenshotPath('hero-mobile')});
    await page.evaluate(() => window.scrollTo(0,document.querySelector('#global-observations').offsetTop-12));
    await page.screenshot({path:screenshotPath('mobile')});
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth+2);
    if (overflow) results.overflow_elements = await page.evaluate(() => [...document.querySelectorAll('body *')]
      .filter(el => el.getBoundingClientRect().right > innerWidth+2 && !el.closest('.table-wrap'))
      .slice(0,12).map(el => ({tag:el.tagName,id:el.id,class:el.className,right:el.getBoundingClientRect().right})));
    assert.equal(overflow,false,'mobile page has horizontal overflow outside table scroller');
    results.mobile = true;
    const fallback = await browser.newPage();
    await fallback.addInitScript(() => {
      const old = HTMLCanvasElement.prototype.getContext;
      HTMLCanvasElement.prototype.getContext = function(type,...args) { return /webgl/.test(type) ? null : old.call(this,type,...args); };
    });
    await fallback.goto(process.env.GAPI_TEST_URL || 'http://127.0.0.1:5174/', {waitUntil:'networkidle'});
    await fallback.locator('#global-show-network').click();
    await fallback.waitForFunction(() => /暂不可用/.test(document.querySelector('#global-network')?.textContent || ''));
    assert.ok(await fallback.locator('#global-rows button').count()>0);
    results.webgl_fallback = true;
    await fallback.close();
    // A valid SHA binds hostile bytes as well as safe bytes. Verify the schema
    // gate and escaped labels in a real browser, without publishing fixtures.
    const publicData=await page.evaluate(async()=>{
      const base=new URL('.',location.href),m=await(await fetch(new URL('data/global/manifest.json',base))).json();
      return(await fetch(new URL('data/global/'+m.snapshot,base))).json();
    });
    for(const unsafeNumber of [true,false]){
      const fixture=structuredClone(publicData);
      fixture.nodes[0][unsafeNumber?'relations':'name']='<img src=x onerror="window.__gapiInjected=1">';
      const bytes=Buffer.from(JSON.stringify(fixture)),sha=createHash('sha256').update(bytes).digest('hex');
      const hostile=await browser.newPage();
      await hostile.route('**/data/global/**',route=>route.fulfill({status:200,contentType:'application/json',body:route.request().url().includes('manifest.json')?JSON.stringify({snapshot:'attack.json',output_sha256:sha}):bytes}));
      await hostile.goto(results.tested_url,{waitUntil:'networkidle'});
      await hostile.waitForFunction(()=>/版本已校验|真实数据暂不可用/.test(document.querySelector('#global-status')?.textContent||''));
      if(unsafeNumber)assert.match(await hostile.locator('#global-status').innerText(),/真实数据暂不可用/);
      else{await hostile.locator('#global-search').fill('<img');assert.ok((await hostile.locator('#global-rows').innerText()).includes('<img'));assert.equal(await hostile.locator('#global-rows img').count(),0);}
      assert.equal(await hostile.evaluate(()=>window.__gapiInjected),undefined);
      await hostile.close();
    }
    results.hostile_number_rejected=true;results.hostile_label_escaped=true;
    assert.deepEqual(errors,[]);
    results.page_errors = errors;
  } finally {
    await browser.close();
    fs.writeFileSync(receiptPath,JSON.stringify(results,null,2));
  }
  console.log(JSON.stringify(results));
})().catch(error => {console.error(error.stack);process.exitCode=1;});
