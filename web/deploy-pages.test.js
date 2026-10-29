import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {deploymentIdentity,capture,ensurePagesBuild} from '../scripts/deploy-pages.mjs';

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
