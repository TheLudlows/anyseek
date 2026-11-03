import { test, expect } from '@playwright/test';

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
