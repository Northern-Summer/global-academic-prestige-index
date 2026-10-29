// Automatic, bounded legacy-Pages transport. Business gates live in verifyBuild.
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {verifyBuild} from './build-provenance.mjs';

const hash=body=>createHash('sha256').update(body).digest('hex');
const REPOSITORY='Northern-Summer/global-academic-prestige-index';
const SOURCE='refs/heads/codex/global-streaming';
const SITE='https://northern-summer.github.io/global-academic-prestige-index/';

export function deploymentIdentity(env,head) {
  if(env.GITHUB_ACTIONS!=='true'||env.GITHUB_EVENT_NAME!=='push'||env.GITHUB_REF!==SOURCE
      ||env.GITHUB_REPOSITORY!==REPOSITORY||env.GITHUB_SHA!==head) throw new Error('Automatic deployment requires the exact trusted push revision');
  return true;
}

export function capture(proof,outDir) {
  const files=new Map();
  for(const [path,expected] of Object.entries(proof.site_manifest.files)) {
    const bytes=path==='.nojekyll'?Buffer.alloc(0):readFileSync(resolve(outDir,path));
    if(hash(bytes)!==expected)throw new Error(`Artifact changed after verification: ${path}`);
    files.set(path,bytes);
  }
  files.set('site-manifest.json',Buffer.from(JSON.stringify(proof.site_manifest)));
  return files;
}

export async function ensurePagesBuild(api,siteCommit,onPrepared=()=>{}) {
  const pages=await api('GET','/pages');
  if(pages.source?.branch!=='gh-pages'||pages.source?.path!=='/'||(pages.build_type&&pages.build_type!=='legacy'))
    throw new Error('Existing Pages configuration changed; refusing to replace it');
  const latest=await api('GET','/pages/builds/latest');
  if(latest?.commit===siteCommit&&['built','queued','building'].includes(latest.status))return {state:'existing-build-confirmed'};
  onPrepared();
  try {await api('POST','/pages/builds');return {state:'build-request-accepted'};}
  catch(error) {
    const settled=await api('GET','/pages/builds/latest');
    if(settled?.commit===siteCommit&&['built','queued','building'].includes(settled.status))return {state:'ambiguous-request-reconciled'};
    throw new Error('Pages build request outcome unknown; do not repeat blindly');
  }
}

export async function fetchBounded(url,maximum,{fetchImpl=fetch,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)),onRetry=()=>{}}={}) {
  // Only repeat idempotent public reads. Authentication, content bounds and
  // hash checks never become retryable; writes use their own reconciliation.
  for(let attempt=0;attempt<3;attempt++){
    try{
      const response=await fetchImpl(url,{signal:AbortSignal.timeout(10000),headers:{'Cache-Control':'no-cache'}});
      if(!response.ok){
        await response.body?.cancel();
        const error=new Error(`Public readback HTTP ${response.status}`);
        error.transientPublicRead=[429,500,502,503,504].includes(response.status);throw error;
      }
      const chunks=[];let length=0;
      for await(const chunk of response.body){length+=chunk.length;if(length>maximum)throw new Error('Public readback exceeds byte bound');chunks.push(chunk);}
      return Buffer.concat(chunks);
    }catch(error){
      if(attempt===2||!(error.transientPublicRead||['TypeError','TimeoutError','AbortError'].includes(error.name)))throw error;
      onRetry({attempt:attempt+1,error:String(error.message).slice(0,160)});
      await sleep((attempt+1)*1000);
    }
  }
}

async function main(){
  const root=resolve(fileURLToPath(new URL('..',import.meta.url))),outDir=join(root,'dist');
  const git=(args,input,env)=>execFileSync('git',args,{cwd:root,input,env:env??process.env,maxBuffer:2*1024*1024,stdio:['pipe','pipe','pipe']}).toString().trim();
  const head=git(['rev-parse','HEAD']);deploymentIdentity(process.env,head);
  const proof=verifyBuild({root,outDir,manifestPath:'outputs/global/manifest.json',publish:true});
  const files=capture(proof,outDir);
  if(!process.env.GITHUB_TOKEN)throw new Error('Scoped Actions token unavailable');
  const api=async(method,path)=>{
    const response=await fetch(`https://api.github.com/repos/${REPOSITORY}${path}`,{method,redirect:'error',
      signal:AbortSignal.timeout(20000),headers:{'Authorization':`Bearer ${process.env.GITHUB_TOKEN}`,
        'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}});
    if(!response.ok)throw new Error(`Pages API ${method} failed with HTTP ${response.status}`);
    let body='';for await(const chunk of response.body){body+=Buffer.from(chunk).toString('utf8');if(body.length>1024*1024)throw new Error('Pages API response exceeds bound');}
    return body?JSON.parse(body):null;
  };
  const receipt={schema_version:'gapi-automatic-deployment/v1',state:'prepared',source_commit:head,
    snapshot_sha256:JSON.parse(readFileSync(join(outDir,'data/global/manifest.json'))).output_sha256,
    site_url:SITE,site_manifest:proof.site_manifest};
  const save=()=>writeFileSync(join(root,'outputs/automatic-deployment.json'),JSON.stringify(receipt,null,2));
  save();
  const remoteHead=git(['ls-remote','origin',SOURCE]).split(/\s/)[0];
  if(remoteHead!==head)throw new Error('Source advanced; obsolete build will not deploy');
  git(['fetch','--no-tags','--depth=1','origin','gh-pages']);
  const predecessor=git(['rev-parse','FETCH_HEAD']);
  const scratch=mkdtempSync(join(tmpdir(),'gapi-pages-'));const env={...process.env,GIT_INDEX_FILE:join(scratch,'index')};
  try{
    git(['read-tree','--empty'],undefined,env);
    for(const [path,bytes]of files){const sha=git(['hash-object','-w','--stdin'],bytes);git(['update-index','--add','--cacheinfo','100644',sha,path],undefined,env);}
    const tree=git(['write-tree'],undefined,env);let siteCommit=predecessor;
    if(git(['rev-parse',`${predecessor}^{tree}`])!==tree){
      siteCommit=git(['-c','user.name=github-actions[bot]','-c','user.email=41898282+github-actions[bot]@users.noreply.github.com',
        'commit-tree',tree,'-p',predecessor],`Publish verified real observations from ${head}\n`);
      receipt.state='push-prepared';receipt.site_commit=siteCommit;save();
      if(git(['ls-remote','origin',SOURCE]).split(/\s/)[0]!==head)throw new Error('Source advanced before push; deployment superseded');
      // Non-force sibling rejection is the mutation boundary. Never retry a
      // push blindly: reconcile an ambiguous outcome against its exact commit.
      try{git(['push','origin',`${siteCommit}:refs/heads/gh-pages`]);}
      catch(error){if(git(['ls-remote','origin','refs/heads/gh-pages']).split(/\s/)[0]!==siteCommit)throw new Error('Pages push not settled; inspect receipt');}
    }
    if(git(['ls-remote','origin','refs/heads/gh-pages']).split(/\s/)[0]!==siteCommit)throw new Error('Pages ref identity mismatch');
    receipt.site_commit=siteCommit;receipt.state='remote-ref-confirmed-http-pending';save();
    // GITHUB_TOKEN pushes do not trigger Pages builds implicitly. Request the
    // configured legacy build explicitly with the narrowly scoped pages token.
    receipt.pages_build=await ensurePagesBuild(api,siteCommit,()=>{receipt.state='pages-build-request-prepared';save();});save();
    // Pages is asynchronous. This bounded wait belongs to the actual deployment.
    let readback=false;
    for(let attempt=0;attempt<12;attempt++){
      try{const body=await fetchBounded(SITE+'site-manifest.json',1024*1024);
        if(JSON.stringify(JSON.parse(body))===JSON.stringify(proof.site_manifest)){readback=true;break;}}
      catch(error){receipt.last_readback_error=String(error.message).slice(0,200);}
      await new Promise(resolve=>setTimeout(resolve,5000));
    }
    if(!readback)throw new Error('Pages manifest has not settled before bounded deadline');
    for(const [path,bytes]of files){const live=await fetchBounded(SITE+path,Math.max(bytes.length+1,1024),{
      onRetry:retry=>{receipt.asset_readback_retries??=[];receipt.asset_readback_retries.push({path,...retry});save();}});
      if(hash(live)!==hash(bytes))throw new Error(`Public asset hash mismatch: ${path}`);}
    receipt.state='live-site-hashes-confirmed';receipt.checked_files=files.size;save();
    console.log(JSON.stringify({state:receipt.state,source_commit:head,checked_files:files.size,site_url:SITE}));
  }finally{rmSync(scratch,{recursive:true,force:true});}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))main().catch(error=>{console.error(error.message);process.exitCode=1;});
