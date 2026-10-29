/* Run with the already installed Playwright path; never downloads a browser. */
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium } = require(process.env.GAPI_PLAYWRIGHT || 'playwright');

(async () => {
  const browser = await chromium.launch({headless:true, executablePath:process.env.GAPI_BROWSER, args:['--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  const results = {}, errors = [];
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(process.env.GAPI_TEST_URL || 'http://127.0.0.1:5174/', {waitUntil:'networkidle',timeout:30000});
    await page.waitForFunction(() => /observed institutions|unavailable:/.test(document.querySelector('#global-status')?.textContent || ''));
    const status = await page.locator('#global-status').innerText();
    assert.ok(!status.includes('unavailable:'),status);
    results.load = status;
    const initialRows = await page.locator('#global-rows tr').count();
    assert.equal(initialRows,50);
    await page.locator('#global-search').fill('Cambridge');
    await page.waitForTimeout(150);
    assert.ok((await page.locator('#global-rows').innerText()).includes('Cambridge'));
    await page.locator('#global-search').fill('');
    await page.locator('#global-country').selectOption('CN');
    assert.ok((await page.locator('#global-rows').innerText()).includes('CN'));
    await page.locator('#global-country').selectOption('');
    await page.locator('#global-sort').selectOption('name');
    await page.locator('#global-sort').selectOption('score');
    const pick = page.locator('#global-rows button').first();
    await pick.focus(); await page.keyboard.press('Enter');
    assert.match(await page.locator('#global-details').innerText(),/Formal qualification:/);
    results.search_filter_sort_keyboard_details = true;
    const downloadPromise = page.waitForEvent('download');
    await page.locator('#global-csv').click();
    const download = await downloadPromise;
    assert.equal(download.suggestedFilename(),'gapi-real-observed-institutions.csv');
    await download.cancel();
    results.csv_download = true;
    await page.locator('#global-reset').click();
    await page.locator('#global-show-network').click();
    await page.waitForFunction(() => document.querySelector('#global-network canvas') || document.querySelector('#global-network')?.textContent.includes('unavailable'));
    results.webgl = await page.locator('#global-network canvas').count() === 1;
    if (results.webgl) await page.locator('#global-network').screenshot({path:'outputs/global-network-2026-10-29.png'});
    await page.locator('#global-stage').selectOption('postdoc');
    results.direction = await page.locator('#global-edge-status').innerText();
    await page.locator('#global-stage').selectOption('all');
    await page.locator('#global-observations').scrollIntoViewIfNeeded();
    await page.evaluate(() => window.scrollTo(0,document.querySelector('#global-observations').offsetTop-12));
    await page.screenshot({path:'outputs/global-desktop-2026-10-29.png'});
    await page.setViewportSize({width:390,height:844});
    await page.screenshot({path:'outputs/global-mobile-2026-10-29.png'});
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth+2);
    assert.equal(overflow,false,'mobile page has horizontal overflow outside table scroller');
    results.mobile = true;
    const fallback = await browser.newPage();
    await fallback.addInitScript(() => {
      const old = HTMLCanvasElement.prototype.getContext;
      HTMLCanvasElement.prototype.getContext = function(type,...args) { return /webgl/.test(type) ? null : old.call(this,type,...args); };
    });
    await fallback.goto(process.env.GAPI_TEST_URL || 'http://127.0.0.1:5174/', {waitUntil:'networkidle'});
    await fallback.locator('#global-show-network').click();
    await fallback.waitForFunction(() => document.querySelector('#global-network')?.textContent.includes('unavailable'));
    assert.ok(await fallback.locator('#global-rows button').count()>0);
    results.webgl_fallback = true;
    assert.deepEqual(errors,[]);
    results.page_errors = errors;
  } finally {
    await browser.close();
    fs.writeFileSync('outputs/browser-real-2026-10-29.json',JSON.stringify(results,null,2));
  }
  console.log(JSON.stringify(results));
})().catch(error => {console.error(error.message);process.exitCode=1;});
