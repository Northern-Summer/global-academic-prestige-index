// Reuses an installed browser and Playwright; no downloads or account access.
const fs=require('node:fs'),assert=require('node:assert/strict');
const {chromium}=require(process.env.GAPI_PLAYWRIGHT||'playwright');
const url=process.env.GAPI_TEST_URL||'http://127.0.0.1:5174/';
const prefix=process.env.GAPI_DERIVED_PREFIX||'outputs/derived-browser';
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.GAPI_BROWSER,args:['--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 const result={url,errors:[]};
 try{
  const page=await browser.newPage({viewport:{width:1280,height:900}});const started=Date.now();
  page.on('pageerror',e=>result.errors.push(e.message));
  await page.goto(url,{waitUntil:'networkidle',timeout:30000});
  await page.waitForFunction(()=>document.querySelector('#historical-status')?.textContent.includes('文件版本已校验'));
  result.ready_ms=Date.now()-started;assert.ok(result.ready_ms<10000,'historical table readiness exceeds 10 seconds');
  assert.equal(await page.locator('#historical-rows tr').count(),50);
  assert.match(await page.locator('#historical-metrics').innerText(),/2,473,948/);
  const documentation=await page.request.get(new URL('docs/EASY_ORCID_2023_OBSERVATIONS.md',url).href);
  assert.equal(documentation.status(),200);assert.match(await documentation.text(),/798,879/);
  await page.evaluate(()=>document.fonts.ready);
  result.cjk_glyphs=await page.evaluate(async()=>{
   const chars=[...new Set(document.body.innerText.match(/[\u3400-\u9fff]/g)||[])];
   const missing={};
   for(const family of ['GAPI Song','DM Sans','DM Mono']){
    await document.fonts.load(`32px "${family}"`,chars.join(''));
    const canvas=document.createElement('canvas');canvas.width=48;canvas.height=48;
    const context=canvas.getContext('2d');context.font=`32px "${family}"`;
    const paint=text=>{context.clearRect(0,0,48,48);context.fillText(text,2,36);return canvas.toDataURL()};
    const tofu=paint('\uffff');missing[family]=chars.filter(char=>paint(char)===tofu);
   }
   return {checked_characters:chars.length,missing};
  });
  assert.ok(result.cjk_glyphs.checked_characters>50,'glyph check must exercise actual Chinese content');
  for(const [family,missing] of Object.entries(result.cjk_glyphs.missing))assert.deepEqual(missing,[],`${family} has missing Chinese glyphs`);
  result.metric_unit_fonts=await page.locator('#historical-metrics small').evaluateAll(nodes=>nodes.map(node=>getComputedStyle(node).fontFamily));
  assert.equal(result.metric_unit_fonts.length,4);
  for(const family of result.metric_unit_fonts)assert.match(family,/DM Sans/,'metric units must use a bundled CJK-capable face');
  result.typography=await page.locator('#historical-title').evaluate(el=>({family:getComputedStyle(el).fontFamily,weight:getComputedStyle(el).fontWeight}));
  assert.match(result.typography.family,/GAPI Song/);assert.equal(result.typography.weight,'400');
  assert.equal(await page.locator('#page-title em').evaluate(el=>getComputedStyle(el).fontStyle),'italic');
  await page.screenshot({path:`${prefix}-desktop.png`});
  await page.locator('#historical-search').fill('Cambridge');await page.waitForTimeout(100);
  assert.match(await page.locator('#historical-rows').innerText(),/Cambridge/);
  await page.locator('#historical-search').fill('');await page.locator('#historical-country').selectOption('CN');
  assert.match(await page.locator('#historical-rows').innerText(),/中国/);
  await page.locator('#historical-country').selectOption('');await page.locator('#historical-identity').selectOption('resolved_exact_ror');
  assert.match(await page.locator('#historical-page-status').innerText(),/23,313/);
  await page.locator('#historical-next').click();assert.match(await page.locator('#historical-page-status').innerText(),/^51/);
  await page.locator('#historical-prev').click();await page.locator('#historical-sort').selectOption('name');
  await page.locator('#historical-sort').selectOption('score');const pick=page.locator('#historical-rows button').first();
  result.picked=await pick.innerText();await pick.focus();await page.keyboard.press('Enter');
  await page.waitForFunction(()=>document.querySelector('#historical-details')?.textContent.includes('全部直接关系已加载'),{timeout:30000});
  assert.match(await page.locator('#historical-edges').innerText(),/→/);
  result.direct_scope=await page.locator('#historical-edge-status').innerText();
  const chunk=await page.evaluate(async()=>{
   const base=new URL('data/derived/',location.href),m=await(await fetch(new URL('manifest.json',base))).json();
   const rows=await(await fetch(new URL(m.nodes.file,base))).json();const n=rows.find(r=>r[1]==='University of Cambridge');return m.edge_chunks[n[6][0]].file;
  });
  await page.route(`**/data/derived/${chunk}`,route=>route.fulfill({status:503,body:'temporarily unavailable'}));
  await page.locator('#historical-overview').click();await pick.click();
  await page.waitForFunction(()=>document.querySelector('#historical-details')?.textContent.includes('关系未完整加载'));
  assert.match(await page.locator('#historical-edge-status').innerText(),/尚未完整读取/);
  assert.match(await page.locator('#historical-edges').innerText(),/当前范围没有关系/);
  await page.unroute(`**/data/derived/${chunk}`);await pick.click();
  await page.waitForFunction(()=>document.querySelector('#historical-details')?.textContent.includes('全部直接关系已加载'));
  result.failed_chunk_does_not_display_partial_or_stale_results=true;
  await page.locator('#historical-stage').selectOption('doctorate');
  await page.locator('#historical-3d').click();await page.waitForTimeout(500);
  assert.ok(await page.locator('#historical-network canvas').count()||/3D 暂不可用/.test(await page.locator('#historical-network').innerText()));
  const canvas=page.locator('#historical-network canvas');
  if(await canvas.count()){
   result.region_colors=Number(await canvas.getAttribute('data-rendered-colors'));
   result.region_nodes=Number(await canvas.getAttribute('data-region-nodes'));
   assert.ok(result.region_colors>=3,'actual scene must encode multiple known ROR regions');
   assert.ok(result.region_nodes>0,'actual scene must contain known ROR regions');
  }
  await page.locator('#historical-details').screenshot({path:`${prefix}-detail.png`});
  await page.locator('#historical-network').screenshot({path:`${prefix}-network.png`});
  const download=page.waitForEvent('download');await page.locator('#historical-csv').click();assert.match((await download).suggestedFilename(),/2023/);
  await page.locator('#historical-overview').click();assert.match(await page.locator('#historical-edge-status').innerText(),/概览/);
  await page.setViewportSize({width:390,height:844});await page.evaluate(()=>window.scrollTo(0,0));await page.screenshot({path:`${prefix}-mobile.png`});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'mobile page overflows horizontally');
  const fallback=await browser.newPage();await fallback.addInitScript(()=>{HTMLCanvasElement.prototype.getContext=function(){return null;};});
  await fallback.goto(url,{waitUntil:'networkidle'});await fallback.waitForFunction(()=>document.querySelector('#historical-status')?.textContent.includes('文件版本已校验'));
  await fallback.locator('#historical-3d').click();await fallback.waitForTimeout(100);
  assert.match(await fallback.locator('#historical-network').innerText(),/3D 暂不可用/);assert.equal(await fallback.locator('#historical-edges tr').count(),50);await fallback.close();
  assert.deepEqual(result.errors,[]);result.status='passed';
 }finally{fs.writeFileSync(`${prefix}.json`,JSON.stringify(result,null,2));await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
