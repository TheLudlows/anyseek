import { test, expect } from "@playwright/test";

test("namespace connection uses gateway authentication", async ({page}) => {
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
  expect(await page.evaluate(() => sessionStorage.getItem("anyseek.tenant"))).toBe('"workspace_a"');
});
