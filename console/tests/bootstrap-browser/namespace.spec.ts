import { test, expect } from "@playwright/test";

test("namespace connection uses gateway authentication and clears legacy credential state", async ({page}) => {
  await page.addInitScript(() => {
    sessionStorage.setItem("anyseek.apiKey", JSON.stringify("obsolete-user-key"));
    sessionStorage.setItem("anyseek.bootstrap", JSON.stringify({key: "obsolete-issued-key"}));
  });
  const requests: string[] = [];
  await page.route("**/api/v1/**", async route => {
    const request = route.request();
    requests.push(new URL(request.url()).pathname);
    expect(request.headers()).not.toHaveProperty("apikey");
    expect(request.headers()["x-anyseek-tenant"]).toBe("workspace_a");
    await route.fulfill({json: []});
  });
  await page.goto("/");
  await expect(page.locator('input[type="password"]')).toHaveCount(0);
  await page.getByLabel("租户命名空间", {exact: true}).fill("workspace_a");
  await page.getByRole("button", {name: "连接服务", exact: true}).click();
  await expect(page.getByRole("heading", {name: "集合", exact: true})).toBeVisible();
  expect(requests).toEqual(["/api/v1/index"]);
  expect(await page.evaluate(() => sessionStorage.getItem("anyseek.apiKey"))).toBeNull();
  expect(await page.evaluate(() => sessionStorage.getItem("anyseek.bootstrap"))).toBeNull();
  expect(await page.evaluate(() => sessionStorage.getItem("anyseek.tenant"))).toBe('"workspace_a"');
});
