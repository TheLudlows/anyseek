import { test, expect } from '@playwright/test';

for (const language of ['zh', 'en'] as const) {
  test(`missing Web Crypto gives actionable error before credentials are sent (${language})`, async ({ page }) => {
    await page.addInitScript(() => Object.defineProperty(globalThis, 'crypto', { value: {}, configurable: true }));
    let requests = 0;
    await page.route('**/api/v1/**', route => { requests++; return route.abort(); });
    await page.goto('/');
    await page.getByLabel('语言 / Language').selectOption(language);
    await page.getByLabel('Tenant API Key', {exact:true}).fill('test-key');
    await page.getByRole('button', {name:language === 'zh' ? '连接服务' : 'Connect', exact:true}).click();
    await expect(page.locator('form').getByRole('alert')).toContainText('HTTPS');
    await expect(page.locator('form').getByRole('alert')).toContainText('127.0.0.1');
    expect(requests).toBe(0);
  });
}
