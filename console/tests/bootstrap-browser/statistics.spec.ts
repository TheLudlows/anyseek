import { test, expect } from '@playwright/test';

for (const language of ['zh', 'en'] as const) {
  test(`advanced statistics are readable tables in ${language}`, async ({page}) => {
    await page.setViewportSize({width:390,height:844});
    const collection = {id:'docs',name:'docs',schema:[],version:'1',statistics:{
      active_records:0,retained_identities:12,retained_operations:3,retained_source_revisions:2,
      physical_versions:20,checkpoint_bytes:1536,visible_seq:'SEQUENCE',evaluation_time_ms:0,evidence_quarantined:false,
    },limits:{max_records:10000,max_operations:20000,max_journal_bytes:1048576,max_candidates:100}};
    await page.route('**/api/v1/**', route => route.fulfill({contentType:'application/json',body:JSON.stringify([collection]).replace('"SEQUENCE"','9007199254740993')}));
    await page.goto('/');
    await page.getByLabel('语言 / Language').selectOption(language);
    await page.getByLabel('Tenant API Key',{exact:true}).fill('test-key');
    await page.getByRole('button',{name:language==='zh'?'连接服务':'Connect',exact:true}).click();
    await page.getByRole('button',{name:language==='zh'?'结构':'Schema',exact:true}).click();
    const dialog=page.getByRole('dialog');
    await dialog.locator('summary').click();
    const stats=dialog.locator('.collection-statistics');
    await expect(stats.locator('table')).toHaveCount(2);
    await expect(stats.locator('tbody tr')).toHaveCount(13);
    await expect(dialog.locator('.json-view, pre')).toHaveCount(0);
    const row=(name:string)=>stats.getByRole('row').filter({has:page.getByText(name,{exact:true})});
    await expect(row(language==='zh'?'活跃记录':'Active records').locator('td')).toHaveText('0');
    await expect(row(language==='zh'?'可见序号':'Visible sequence').locator('td')).toHaveText('9,007,199,254,740,993');
    await expect(stats).toContainText('1.5 KiB (1,536 B)');
    await expect(stats).toContainText('UTC');
    await expect(row(language==='zh'?'来源与证据隔离':'Source and evidence quarantine').locator('td')).toHaveText(language==='zh'?'未隔离':'Not quarantined');
    expect(await dialog.evaluate(el=>el.scrollWidth<=el.clientWidth)).toBe(true);
    await page.screenshot({path:`test-results/statistics-${language}.png`,fullPage:true});
  });
}

test('missing statistics are not reported as zero or unlimited',async({page})=>{
  await page.route('**/api/v1/**',route=>route.fulfill({json:[{id:'empty',name:'empty',schema:[],statistics:{active_records:0},limits:null}]}));
  await page.goto('/');
  await page.getByLabel('Tenant API Key',{exact:true}).fill('test-key');
  await page.getByRole('button',{name:'连接服务',exact:true}).click();
  await page.getByRole('button',{name:'结构',exact:true}).click();
  await page.getByRole('dialog').locator('summary').click();
  await expect(page.locator('.collection-statistics td').filter({hasText:'未提供'})).toHaveCount(12);
});
