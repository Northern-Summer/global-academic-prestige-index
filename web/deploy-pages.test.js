import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {deploymentIdentity,capture,ensurePagesBuild,fetchBounded,waitForPublicManifest} from '../scripts/deploy-pages.mjs';

test('automatic deployment only accepts exact trusted push identity',()=>{
  const head='a'.repeat(40),env={GITHUB_ACTIONS:'true',GITHUB_EVENT_NAME:'push',
    GITHUB_REF:'refs/heads/codex/global-streaming',GITHUB_REPOSITORY:'Northern-Summer/global-academic-prestige-index',GITHUB_SHA:head};
  assert.equal(deploymentIdentity(env,head),true);
  for(const [key,value]of Object.entries({GITHUB_ACTIONS:'false',GITHUB_EVENT_NAME:'pull_request',GITHUB_REF:'refs/heads/other',GITHUB_REPOSITORY:'other/repo',GITHUB_SHA:'b'.repeat(40)}))
    assert.throws(()=>deploymentIdentity({...env,[key]:value},head));
});

test('legacy Pages is explicitly built once and ambiguous writes are reconciled',async()=>{
  const pages={source:{branch:'gh-pages',path:'/'},build_type:'legacy'};
  for(const mode of ['accepted','already-built','ambiguous-settled','ambiguous-unsettled','wrong-config']){
    const calls=[];let posts=0,reads=0;
    const api=async(method,path)=>{calls.push([method,path]);
      if(path==='/pages')return mode==='wrong-config'?{...pages,build_type:'workflow'}:pages;
      if(method==='GET'){reads++;return {commit:mode==='already-built'||mode==='ambiguous-settled'&&reads>1?'expected':'old',status:'built'};}
      posts++;if(mode.startsWith('ambiguous'))throw new Error('network outcome unknown');return {status:'queued'};
    };
    if(['ambiguous-unsettled','wrong-config'].includes(mode))await assert.rejects(ensurePagesBuild(api,'expected'));
    else await ensurePagesBuild(api,'expected');
    assert.equal(posts,mode==='already-built'||mode==='wrong-config'?0:1);
  }
});

test('artifact byte capture rejects in-flight replacement and does not mutate site files',()=>{
  const folder=mkdtempSync(join(process.env.GAPI_TEST_TMP||tmpdir(),'gapi-capture-test-'));
  try{
    const body=Buffer.from('real verified content'),hash=bytes=>createHash('sha256').update(bytes).digest('hex');
    writeFileSync(join(folder,'index.html'),body);
    const proof={site_manifest:{source_commit:'a'.repeat(40),files:{'index.html':hash(body),'.nojekyll':hash(Buffer.alloc(0))}}};
    const one=capture(proof,folder),two=capture(proof,folder);
    assert.deepEqual([...one],[...two]);assert.equal(one.get('index.html').equals(body),true);
    writeFileSync(join(folder,'index.html'),'replacement');assert.throws(()=>capture(proof,folder),/changed/);
  }finally{rmSync(folder,{recursive:true,force:true});}
});

test('public asset reads survive bounded transient 503 and network errors',async()=>{
  for(const kind of ['http','network']){
    let calls=0;const retries=[];
    const bytes=await fetchBounded('https://example.invalid/site.css',32,{
      fetchImpl:async()=>{calls++;if(calls===1){if(kind==='network')throw new TypeError('fetch failed');return new Response('temporary',{status:503});}return new Response('verified bytes');},
      sleep:async()=>{},onRetry:retry=>retries.push(retry)
    });
    assert.equal(bytes.toString(),'verified bytes');assert.equal(calls,2);assert.equal(retries.length,1);
  }
});

test('public read retry bound preserves permanent and oversized failures',async()=>{
  for(const mode of ['persistent503','forbidden','oversized']){
    let calls=0;
    await assert.rejects(fetchBounded('https://example.invalid/site.css',8,{
      fetchImpl:async()=>{calls++;return mode==='persistent503'?new Response('',{status:503}):mode==='forbidden'?new Response('',{status:403}):new Response('over the byte bound');},sleep:async()=>{}
    }),/HTTP|byte bound/);
    assert.equal(calls,mode==='persistent503'?3:1);
  }
});

test('public manifest settlement accepts a 65.55-second Pages build without repeating writes',async()=>{
  let clock=0,reads=0;const expected={source_commit:'frozen',files:{'index.html':'hash'}},errors=[];
  const settled=await waitForPublicManifest(expected,{
    now:()=>clock,sleep:async ms=>{clock+=ms;},
    read:async()=>{reads++;if(reads===1)throw new TypeError('transient read');return Buffer.from(JSON.stringify(clock<65550?{source_commit:'previous'}:expected));},
    onReadError:error=>errors.push(error.message)
  });
  assert.equal(settled,true);assert.equal(clock,70000);assert.equal(errors.length,1);
  assert.ok(reads<40);
});

test('public settlement stays bounded and rejects stale, malformed and late manifests',async()=>{
  for(const mode of ['stale','malformed','late']){
    let clock=0,calls=0;
    const settled=await waitForPublicManifest({source_commit:'expected'},{timeoutMs:12000,intervalMs:5000,
      now:()=>clock,sleep:async ms=>{clock+=ms;},read:async()=>{
        calls++;if(mode==='late'){clock+=13000;return '{"source_commit":"expected"}';}
        return mode==='malformed'?'invalid json':'{"source_commit":"previous"}';
      }});
    assert.equal(settled,false);assert.equal(clock,mode==='late'?13000:12000);assert.ok(calls<=3);
  }
  await assert.rejects(waitForPublicManifest({}, {timeoutMs:Infinity}),/time bound/);
});
