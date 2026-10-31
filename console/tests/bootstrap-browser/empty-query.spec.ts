import { test, expect } from '@playwright/test';

test('empty and whitespace lexical queries reach the API', async ({ page }) => {
  const queries: Record<string, unknown>[] = [];
  const collection = { id: 'docs', name: 'docs', schema: [{field: 'body', field_type: 'Text', store: true}], version: '1', statistics: { active_records: 0 }, limits: {} };
  await page.route('**/api/v1/**', async route => {
    if (route.request().url().endsWith('/query')) {
      queries.push(route.request().postDataJSON());
      await route.fulfill({json: {hits: [], count_total: 0}});
    } else await route.fulfill({json: [collection]});
  });
  await page.goto('/');
  await page.getByLabel('Tenant API Key', {exact:true}).fill('test-key');
  await page.getByRole('button', {name:'连接服务', exact:true}).click();
  await page.getByRole('button', {name:'检索调试', exact:true}).click();
  await expect(page.getByLabel('检索内容')).toHaveAttribute('placeholder', '输入关键词；留空可浏览记录…');
  for (const query of ['', '   ']) {
    await page.getByLabel('检索内容').fill(query);
    await page.getByRole('button', {name:'运行检索', exact:true}).click();
    await expect.poll(() => queries.length).toBe(query ? 2 : 1);
    expect(queries.at(-1)?.query).toBe(query);
    expect(queries.at(-1)?.query_vector).toBeUndefined();
    await expect(page.getByRole('button', {name:'运行检索', exact:true})).toBeEnabled();
  }
});
