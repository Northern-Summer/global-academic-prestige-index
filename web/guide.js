import './style.css';
import './scores.css';
import {shareExplanation,yearExplanation,sourceEdition} from './public-content.js';
import {fetchJSON} from './derived-data.js';
import {validatePartitionManifest,fetchPartitionAsset} from './global-partitions.js';
import {processingText} from './record-processing.js';
document.querySelector('[data-copy="share"]').textContent=shareExplanation;
document.querySelector('[data-copy="years"]').textContent=yearExplanation;
const box=document.querySelector('#guide-processing'),retry=document.querySelector('#guide-retry');
async function load(){
 retry.hidden=true;box.textContent='正在读取本版数据范围…';
 try{
  const base=new URL('data/global/',new URL(import.meta.env.BASE_URL,location.origin));
  const {value:m}=await fetchJSON(new URL('manifest.json',base),1024*1024,AbortSignal.timeout(120000));
  validatePartitionManifest(m);
  const field=m.fields.find(f=>f.field==='coverage');
  if(!field?.asset)throw Error('coverage descriptor absent');
  const coverage=await fetchPartitionAsset(base,field.asset,{signal:AbortSignal.timeout(120000)});
  box.replaceChildren(...processingText(coverage).map(text=>{const p=document.createElement('p');p.textContent=text;return p;}));
  const scopeField=m.fields.find(f=>f.field==='scope');
  const scope=scopeField?.asset?await fetchPartitionAsset(base,scopeField.asset,{signal:AbortSignal.timeout(120000)}):{};
  const edition=sourceEdition(m,scope);
  document.querySelector('#guide-version').textContent=`${edition.release?'当前发布：'+edition.release+'。':''}本版来源：${edition.source}。${edition.dates.length?'年度快照日期：'+edition.dates.join('、')+'。':''}2023 数据保留为历史对照。`;
 }catch(error){console.error('GAPI data explanation load failure',error);box.textContent='本版统计暂时无法读取。上方的来源与方法说明仍可阅读，请检查网络后重试。';retry.hidden=false;}
}
retry.onclick=load;load();
