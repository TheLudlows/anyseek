import { test, expect } from '@playwright/test';
import { createHash } from 'node:crypto';

for (const language of ['zh', 'en'] as const) {
  test(`progress, errors, recovery, capacity and failed refresh (${language})`, async ({page}) => {
    await page.setViewportSize({width:390,height:844});
    const collection = {id:'docs',name:'docs',schema:[],statistics:{active_records:1,retained_identities:12,retained_operations:20},limits:{max_records:100,max_operations:100}};
    let progress: any = {committed_seq:'COMMIT',lexical_seq:'LEXICAL',vector_seq:'COMMIT',index_error:null,recovery_required:false};
    let failed = false;
    await page.route('**/api/v1/**', route => {
      const path = new URL(route.request().url()).pathname;
      if (failed && path.endsWith('/ingest')) return route.fulfill({status:503,json:{code:'busy',message:'status temporarily unavailable'}});
      const value = path.endsWith('/apikey') ? [collection] : path.endsWith('/ingest') ? progress : collection;
      return route.fulfill({contentType:'application/json',body:JSON.stringify(value).replaceAll('"COMMIT"','9007199254740993').replaceAll('"LEXICAL"','9007199254740992')});
    });
    await page.goto('/');
    await page.getByLabel('语言 / Language').selectOption(language);
    await page.getByLabel('Tenant API Key',{exact:true}).fill('test-key');
    await page.getByRole('button',{name:language==='zh'?'连接服务':'Connect',exact:true}).click();
    await page.getByRole('button',{name:language==='zh'?'结构':'Schema',exact:true}).click();
    const dialog=page.getByRole('dialog');
    const runtime=dialog.getByRole('region',{name:language==='zh'?'运行状态':'Runtime status',exact:true});
    await expect(runtime).toContainText(language==='zh'?'索引尚未追平':'Indexes are catching up');
    await expect(runtime.getByRole('table',{name:language==='zh'?'索引进度':'Index progress'})).toContainText('9,007,199,254,740,993');
    await dialog.locator('summary').click();
    const capacity=runtime.getByRole('table',{name:language==='zh'?'容量余量':'Remaining capacity'});
    await expect(capacity.locator('td')).toHaveText(['88','80',language==='zh'?'未提供':'Not provided']);
    const refresh=runtime.getByRole('button',{name:language==='zh'?'刷新运行状态':'Refresh runtime status'});
    progress={...progress,index_error:'cannot publish segment <failed>'};
    await refresh.click();
    await expect(runtime.getByRole('alert').filter({hasText:'cannot publish'})).toContainText('<failed>');
    progress={...progress,committed_seq:null,index_error:null,recovery_required:true};
    await refresh.click();
    await expect(runtime).toContainText(language==='zh'?'需要恢复':'Recovery required');
    await expect(runtime.getByRole('row').filter({hasText:language==='zh'?'持久提交位置':'Durable commit position'}).locator('td')).toHaveText(language==='zh'?'未提供':'Not provided');
    progress={committed_seq:0,lexical_seq:0,vector_seq:0,index_error:null,recovery_required:false};
    await refresh.click();
    await expect(runtime).toContainText(language==='zh'?'索引已追平此提交快照':'Indexes caught up to this commit snapshot');
    failed=true;
    await refresh.click();
    await expect(runtime).toContainText('status temporarily unavailable');
    await expect(runtime).toContainText(language==='zh'?'索引状态未知':'Index status unknown');
    await expect(runtime).not.toContainText(language==='zh'?'索引已追平此提交快照':'Indexes caught up to this commit snapshot');
    expect(await dialog.evaluate(el=>el.scrollWidth<=el.clientWidth)).toBe(true);
    await page.screenshot({path:`test-results/runtime-status-${language}.png`,fullPage:true});
  });
}

test('ingest acknowledgement replaces the old snapshot without claiming new writes are searchable', async ({page}) => {
  const collection={id:'docs',name:'docs',schema:[],statistics:{active_records:0},limits:{}};
  const operation={epoch:Math.floor(Date.now()/86400000),nonce:'original-operation'};
  const original='{"operation_id":'+JSON.stringify(operation)+',"record_id":"original-record","expected":"Absent","document":{"n":9007199254740993}}';
  let committed=0, posted='';
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  await page.route('**/api/v1/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    if(path.endsWith('/ingest')) {
      if(route.request().method()==='POST') {
        posted=route.request().postData()!;
        await gate;
        committed=1;
        return route.fulfill({json:{results:[{code:'committed',operation_id:operation,receipt:{record_id:'original-record',seq:1}}]}});
      }
      return route.fulfill({json:{committed_seq:committed,lexical_seq:0,vector_seq:0,index_error:null,recovery_required:false}});
    }
    return route.fulfill({json:path.endsWith('/iterator')?{records:[],next:null}:path.endsWith('/apikey')?[collection]:collection});
  });
  await page.goto('/');
  await page.getByLabel('Tenant API Key',{exact:true}).fill('test-key');
  await page.getByRole('button',{name:'连接服务',exact:true}).click();
  await page.evaluate(pending=>sessionStorage.setItem('anyseek.pendingWrites',JSON.stringify(pending)),{
    owner:createHash('sha256').update('test-key').digest('hex'),collection:'docs',delivery:'ingest',
    completed:0,unresolved:true,requests:[{body:original,outcome:'unknown'}],
  });
  await page.getByRole('navigation',{name:'主导航'}).getByRole('button',{name:'数据',exact:true}).click();
  await page.locator('.page-heading').getByRole('button',{name:'导入数据',exact:true}).click();
  const dialog=page.getByRole('dialog');
  await expect(dialog).toContainText('索引已追平此提交快照');
  await dialog.getByRole('button',{name:'重试原始写入'}).click();
  await expect.poll(()=>posted).not.toBe('');
  await expect(dialog.getByRole('region',{name:'运行状态'})).toHaveCount(0);
  expect(posted).toBe('{"records":['+original+']}');
  release();
  await expect(dialog).toContainText('已确认提交 1 条，索引正在异步更新。');
  await expect(dialog).toContainText('索引尚未追平');
  await expect(dialog).not.toContainText('索引已追平此提交快照');
});
