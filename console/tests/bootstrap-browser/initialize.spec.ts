import { test, expect, type Page } from "@playwright/test";
const master = "mock-master-only";
async function open(page: Page) {
  await page.goto("/");
  await page
    .getByRole("button", { name: "首次使用？使用 Master Key 初始化" })
    .click();
}
async function fill(page: Page) {
  await page.getByLabel("Tenant 名称", { exact: true }).fill("new_tenant");
  await page.getByLabel("Master Key", { exact: true }).fill(master);
  await page.getByRole("checkbox").check();
}
for (const fail of ["none", "tenant", "key", "connect"]) {
  test(`initialize and retry ${fail}`, async ({ page }) => {
    const calls: { path: string; body: string | null; auth: string }[] = [];
    let failed = false;
    await page.route("**/api/v1/**", async (route) => {
      const req = route.request();
      const path = req.url().split("/api/v1/")[1];
      const body = req.postData();
      calls.push({ path, body, auth: req.headers().apikey });
      const stage = body ? (path === "tenants" ? "tenant" : "key") : "connect";
      if (stage === fail && !failed) {
        failed = true;
        await route.abort();
        return;
      }
      await route.fulfill({ json: body ? { seq: 1 } : [] });
    });
    await open(page);
    await fill(page);
    await page.getByRole("button", { name: "确认初始化", exact: true }).click();
    if (fail === "tenant" || fail === "key") {
      await expect(page.getByRole("alert").last()).toBeVisible();
      await page.getByRole("button", { name: "返回普通 Key 连接" }).click();
      await page.reload();
      await page
        .getByRole("button", { name: "首次使用？使用 Master Key 初始化" })
        .click();
      await expect(page.getByLabel("Master Key", { exact: true })).toHaveValue(
        "",
      );
      await page.getByLabel("Master Key", { exact: true }).fill(master);
      await page.getByRole("checkbox").check();
      await page.getByRole("button", { name: "重试原初始化请求" }).click();
      const retried = calls.filter(
        (c) => c.path === (fail === "tenant" ? "tenants" : "apikey") && c.body,
      );
      expect(retried[0].body).toBe(retried[1].body);
    }
    await expect(page.getByLabel("新 Tenant API Key")).toBeVisible();
    const key = await page.getByLabel("新 Tenant API Key").inputValue();
    await page.getByRole("checkbox").check();
    await page.getByRole("button", { name: "进入工作区", exact: true }).click();
    if (fail === "connect") {
      await expect(page.getByRole("alert").last()).toBeVisible();
      await page
        .getByRole("button", { name: "进入工作区", exact: true })
        .click();
    }
    await expect(page.getByRole("heading", { name: "所有集合" })).toBeVisible();
    expect(calls.filter((c) => !c.body).every((c) => c.auth === key)).toBe(
      true,
    );
    expect(calls.filter((c) => c.body).every((c) => c.auth === master)).toBe(
      true,
    );
    const stored = await page.evaluate(() =>
      JSON.stringify({ ...sessionStorage, ...localStorage }),
    );
    expect(stored).not.toContain(master);
    expect(
      await page.evaluate(() => sessionStorage.getItem("anyseek.bootstrap")),
    ).toBeNull();
  });
}
test("cancel before confirmation and ordinary key connection", async ({
  page,
}) => {
  const methods: string[] = [];
  await page.route("**/api/v1/**", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ json: [] });
  });
  await open(page);
  await fill(page);
  await page.getByRole("button", { name: "返回普通 Key 连接" }).click();
  expect(methods).toEqual([]);
  await page
    .getByLabel("Tenant API Key", { exact: true })
    .fill("existing-ordinary-key");
  await page.getByRole("button", { name: "连接服务", exact: true }).click();
  await expect(page.getByRole("heading", { name: "所有集合" })).toBeVisible();
  expect(methods).toEqual(["GET"]);
});
test("duplicate submit is suppressed and tenant conflict never signs a key", async ({
  page,
}) => {
  let count = 0;
  await page.route("**/api/v1/**", async (route) => {
    count++;
    await new Promise((resolve) => setTimeout(resolve, 200));
    await route.fulfill({
      status: 409,
      json: { code: "conflict", message: "tenant exists" },
    });
  });
  await open(page);
  await fill(page);
  await page
    .getByRole("button", { name: "确认初始化", exact: true })
    .evaluate((button) => {
      const form = button.closest("form")!;
      form.dispatchEvent(
        new Event("submit", { bubbles: true, cancelable: true }),
      );
      form.dispatchEvent(
        new Event("submit", { bubbles: true, cancelable: true }),
      );
    });
  await expect(page.getByRole("alert").last()).toContainText("初始化发生冲突");
  expect(count).toBe(1);
  await expect(page.getByLabel("新 Tenant API Key")).toHaveCount(0);
});

test("master rejection preserves request and mobile initialization fits viewport", async ({
  page,
}) => {
  const bodies: (string | null)[] = [];
  let rejected = false;
  await page.route("**/api/v1/**", async (route) => {
    bodies.push(route.request().postData());
    if (!rejected) {
      rejected = true;
      await route.fulfill({ status: 401, json: { message: master } });
    } else await route.fulfill({ json: { seq: 1 } });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  await fill(page);
  await page.getByRole("button", { name: "确认初始化", exact: true }).click();
  await expect(page.getByRole("alert").last()).toContainText("Key 无效");
  expect(
    await page.evaluate(() =>
      JSON.stringify({ ...sessionStorage, ...localStorage }),
    ),
  ).not.toContain(master);
  expect(await page.locator("body").innerText()).not.toContain(master);
  await page.getByRole("button", { name: "重试原初始化请求" }).click();
  await expect(page.getByLabel("新 Tenant API Key")).toBeVisible();
  expect(bodies[0]).toBe(bodies[1]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/bootstrap-mobile.png",
    fullPage: true,
  });
});
