import { test, expect, type Page } from "@playwright/test";
import { randomBytes } from "node:crypto";
import { mkdir } from "node:fs/promises";

const login = async (page: Page) => {
  await page.goto("/");
  await page
    .getByLabel("Tenant API Key", { exact: true })
    .fill(process.env.ANYSEEK_TEST_KEY!);
  await page.getByRole("button", { name: "连接服务", exact: true }).click();
  await expect(page.getByRole("heading", { name: "所有集合" })).toBeVisible();
};
const operation = () => ({
  epoch: Math.floor(Date.now() / 86400000),
  nonce: randomBytes(16).toString("hex"),
});
const call = async (path: string, body?: object) => {
  const response = await fetch("http://127.0.0.1:18180/api/v1/" + path, {
    method: body ? "POST" : "GET",
    headers: {
      apikey: process.env.ANYSEEK_TEST_KEY!,
      "Content-Type": "application/json",
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  expect(response.status, await response.clone().text()).toBe(200);
  return response.json();
};

test("API-only kernel, connection, real collection list, responsive navigation and screenshots", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  for (const path of ["/", "/js/master.js", "/css/master.css", "/svg/logo.svg"])
    expect((await fetch("http://127.0.0.1:18180" + path)).status).toBe(404);
  await page.goto("/");
  await mkdir("test-results/screenshots", { recursive: true });
  await page.screenshot({
    path: "test-results/screenshots/welcome.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByLabel("Tenant API Key", { exact: true }).fill("invalid");
  await page.getByRole("button", { name: "连接服务", exact: true }).click();
  await expect(page.getByRole("alert").first()).toBeVisible();
  await login(page);
  await expect(page.getByRole("table")).toContainText("knowledge_base");
  await page.screenshot({
    path: "test-results/screenshots/collections.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByLabel("筛选集合").fill("missing");
  await expect(
    page.getByRole("heading", { name: "没有匹配的集合" }),
  ).toBeVisible();
  await page.getByLabel("筛选集合").clear();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "打开导航" }).click();
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "数据", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "数据", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("table")).toContainText("guide_1");
  await page.screenshot({
    path: "test-results/screenshots/mobile-data.png",
    fullPage: true,
    animations: "disabled",
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(errors).toEqual([]);
});

test("creation and i64 import survive lost responses and reload with identical requests; exact display/copy, cursor and conditional edit", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await login(page);
  const creations: string[] = [];
  await page.route(
    "**/api/v1/index",
    async (route) => {
      creations.push(route.request().postData()!);
      const response = await route.fetch();
      expect(response.status()).toBe(200);
      await route.abort("failed");
    },
    { times: 1 },
  );
  await page
    .getByRole("button", { name: "创建集合", exact: true })
    .first()
    .click();
  await page.getByLabel("集合名称", { exact: true }).fill("browser_records");
  await page.getByLabel("字段 3 名称", { exact: true }).fill("rank");
  await page.getByLabel("字段 3 类型", { exact: true }).selectOption("Int");
  await page.getByLabel("字段 3 过滤排序", { exact: true }).check();
  await page.getByRole("button", { name: "添加字段", exact: true }).click();
  await page.getByLabel("字段 4 名称", { exact: true }).fill("measure");
  await page.getByLabel("字段 4 类型", { exact: true }).selectOption("Float");
  await page.getByRole("button", { name: "添加字段", exact: true }).click();
  await page.getByLabel("字段 5 名称", { exact: true }).fill("data");
  await page.getByLabel("字段 5 类型", { exact: true }).selectOption("Json");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "创建集合", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "重试原始创建" }),
  ).toBeVisible();
  await expect(page.getByLabel("集合名称", { exact: true })).toBeDisabled();
  await page.reload();
  await page
    .getByRole("button", { name: "创建集合", exact: true })
    .first()
    .click();
  await page.route(
    "**/api/v1/index",
    async (route) => {
      creations.push(route.request().postData()!);
      await route.continue();
    },
    { times: 1 },
  );
  await page.getByRole("button", { name: "重试原始创建" }).click();
  await expect(
    page.getByRole("heading", { name: "数据", exact: true }),
  ).toBeVisible();
  expect(creations[0]).toBe(creations[1]);
  const docs = [
    "9223372036854775807",
    "-9223372036854775808",
    "9007199254740993",
  ].map(
    (n, i) =>
      `{"title":"precision ${i}","body":"precision","rank":${n},"measure":1.2500e0,"data":{"n":${n}}}`,
  );
  await page.locator(".page-heading").filter({ has: page.getByRole("heading", { name: "数据", exact: true }) }).getByRole("button", { name: "导入数据", exact: true }).click();
  await page
    .getByLabel("导入 JSON", { exact: true })
    .fill("[" + docs.join(",") + "]");
  const writes: string[] = [];
  await page.route(
    "**/api/v1/index/browser_records/doc",
    async (route) => {
      writes.push(route.request().postData()!);
      const response = await route.fetch();
      expect(response.status()).toBe(200);
      await route.abort("failed");
    },
    { times: 1 },
  );
  await page.getByRole("button", { name: "写入集合", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "重试原始写入" }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "数据", exact: true }),
  ).toBeVisible();
  await page.locator(".page-heading").filter({ has: page.getByRole("heading", { name: "数据", exact: true }) }).getByRole("button", { name: "导入数据", exact: true }).click();
  await expect(page.getByLabel("导入 JSON", { exact: true })).toBeDisabled();
  await page.route("**/api/v1/index/browser_records/doc", async (route) => {
    writes.push(route.request().postData()!);
    await route.continue();
  });
  await page.getByRole("button", { name: "重试原始写入" }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "已确认写入 3 条" }),
  ).toBeVisible();
  expect(writes[0]).toBe(writes[1]);
  writes
    .slice(1)
    .forEach((body, i) =>
      expect(body.endsWith('"document":' + docs[i] + "}")).toBe(true),
    );
  await page
    .getByRole("dialog")
    .locator(".modal-actions")
    .getByRole("button", { name: "关闭", exact: true })
    .click();
  await expect(page.getByRole("table")).toContainText("9223372036854775807");
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "检索调试", exact: true })
    .click();
  await page.getByRole("textbox", { name: "检索内容" }).fill("precision");
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await page.locator(".result-card").filter({ hasText: "precision 0" }).click();
  const preview = page.getByRole("complementary", { name: "记录详情" });
  await expect(preview.locator("pre").first()).toContainText(
    "9223372036854775807",
  );
  await expect(preview.locator("pre").first()).toContainText("1.25");
  await preview
    .getByRole("button", { name: "复制 Document JSON", exact: true })
    .click();
  const copied = await page.evaluate(() => navigator.clipboard.readText());
  expect(copied).toBe(docs[0].replace("1.2500e0", "1.25"));
  const first = JSON.parse(writes[0]);
  const original = await call("index/browser_records/doc/" + first.record_id);
  expect(original.record_id).toBe(first.record_id);
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "数据", exact: true })
    .click();
  await page.locator(".page-heading").filter({ has: page.getByRole("heading", { name: "数据", exact: true }) }).getByRole("button", { name: "导入数据", exact: true }).click();
  await page.getByLabel("Record envelope 模式").check();
  await page
    .getByLabel("导入 JSON", { exact: true })
    .fill(
      `{"record_id":"${first.record_id}","routing_key":"${first.routing_key}","expected":{"Match":1},"document":${copied}}`,
    );
  await page.getByRole("button", { name: "写入集合", exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "已确认写入 1 条" }),
  ).toBeVisible();
  expect(
    (await call("index/browser_records/doc/" + first.record_id)).version,
  ).toBe(2);
});

test("real lexical/vector/hybrid search, empty state, explanations, advanced tools and screenshots", async ({
  page,
}) => {
  await login(page);
  await page
    .getByLabel("当前集合", { exact: true })
    .selectOption("knowledge_base");
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "检索调试", exact: true })
    .click();
  await page.getByRole("textbox", { name: "检索内容" }).fill("检索");
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await expect(page.locator(".result-card").first()).toBeVisible();
  await page.locator(".result-card").first().click();
  await expect(
    page.getByRole("complementary", { name: "记录详情" }),
  ).toContainText("lexical");
  await page.screenshot({
    path: "test-results/screenshots/search.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByRole("button", { name: "向量 Vector", exact: true }).click();
  await page.getByRole("textbox", { name: "查询向量" }).fill("[1,0,0]");
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await expect(page.locator(".result-card")).toHaveCount(4);
  await page.getByRole("button", { name: "混合 Hybrid", exact: true }).click();
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await expect(page.locator(".result-card").first()).toBeVisible();
  await page
    .getByRole("button", { name: "关键词 Lexical", exact: true })
    .click();
  await page
    .getByRole("textbox", { name: "检索内容" })
    .fill("zzzzmissingzzzzz");
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "没有找到匹配记录" }),
  ).toBeVisible();
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "数据", exact: true })
    .click();
  await page.getByRole("button", { name: "高级检查", exact: true }).click();
  await page.getByRole("button", { name: "运行检查", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText("records");
  await page.getByRole("button", { name: "来源证据", exact: true }).click();
  await expect(page.getByLabel("请求 JSON")).toContainText("locator");
});

test("real cursor pagination, relation/evidence tools and administrator backup/restore", async ({
  page,
}) => {
  const collection = "product_docs";
  for (const id of ["entity_a", "entity_b"])
    await call(`index/${collection}/doc`, {
      operation_id: operation(),
      record_id: id,
      routing_key: "route",
      expected: "Absent",
      document: { title: id, body: "entity" },
      system: { kind: "entity" },
    });
  await call(`index/${collection}/doc`, {
    operation_id: operation(),
    record_id: "edge",
    routing_key: "route",
    expected: "Absent",
    document: { title: "Knows", body: "relation" },
    system: {
      kind: "relation",
      relation: {
        from: { record_id: "entity_a", routing_key: "route", version: 1 },
        to: { record_id: "entity_b", routing_key: "route", version: 1 },
        relation_type: "knows",
      },
    },
  });
  await call(`index/${collection}/doc`, {
    operation_id: operation(),
    record_id: "source",
    routing_key: "route",
    expected: "Absent",
    document: { title: "Source", body: "sourceword" },
    system: { kind: "source" },
  });
  for (let i = 0; i < 20; i++)
    await call(`index/${collection}/doc`, {
      operation_id: operation(),
      record_id: "page_" + i.toString().padStart(2, "0"),
      routing_key: "route",
      expected: "Absent",
      document: { title: "分页 " + i, body: "pagination" },
    });
  await login(page);
  await page.getByLabel("当前集合", { exact: true }).selectOption(collection);
  await page
    .getByRole("navigation", { name: "主导航" })
    .getByRole("button", { name: "数据", exact: true })
    .click();
  await expect(page.locator("tbody tr")).toHaveCount(20);
  const firstIds = await page.locator(".record-link").allTextContents();
  await page.getByRole("button", { name: "下一页记录", exact: true }).click();
  await expect(page.getByText("第 2 页", { exact: true })).toBeVisible();
  await expect(page.locator("tbody tr")).toHaveCount(4);
  const secondIds = await page.locator(".record-link").allTextContents();
  expect(secondIds.filter((id) => firstIds.includes(id))).toEqual([]);
  await page.getByRole("button", { name: "高级检查", exact: true }).click();
  await page.getByRole("button", { name: "实体目录", exact: true }).click();
  await page.getByRole("button", { name: "运行检查", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText("entity_a");
  await page.getByRole("button", { name: "一跳关系", exact: true }).click();
  await page.getByLabel("请求 JSON", { exact: true }).fill(
    JSON.stringify({
      seeds: [{ record_id: "entity_a", routing_key: "route", version: 1 }],
      direction: "out",
      relation_types: [],
      per_seed: 10,
      max_candidates: 100,
      allow_partial: false,
    }),
  );
  await page.getByRole("button", { name: "运行检查", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText("entity_b");
  await page.getByRole("button", { name: "关系融合", exact: true }).click();
  await page.getByLabel("请求 JSON", { exact: true }).fill(
    JSON.stringify({
      query: "relation",
      length: 10,
      relation_fusion: {
        rrf: {
          constant: 3,
          keyword_weight: 1,
          vector_weight: 0,
          keyword_candidates: 100,
        },
        relation_weight: 1,
        neighbors: {
          seeds: [{ record_id: "entity_a", routing_key: "route", version: 1 }],
          direction: "out",
          relation_types: [],
          per_seed: 10,
          max_candidates: 100,
          allow_partial: false,
        },
      },
    }),
  );
  await page.getByRole("button", { name: "运行检查", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText(
    "candidate_window",
  );
  await page.getByRole("button", { name: "来源证据", exact: true }).click();
  await page.getByLabel("请求 JSON", { exact: true }).fill(
    JSON.stringify({
      record_id: "source",
      routing_key: "route",
      version: 1,
      locator: { kind: "utf8", field: "body", start: 0, end: 6 },
    }),
  );
  await page.getByRole("button", { name: "运行检查", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText("content_sha256");
  await expect(page.locator(".json-view pre")).toContainText("source");
  await page.getByRole("button", { name: "连接与管理", exact: true }).click();
  await page
    .getByLabel("Master API Key", { exact: true })
    .fill(process.env.ANYSEEK_TEST_MASTER!);
  await page
    .getByRole("button", { name: "创建 / 重试备份", exact: true })
    .click();
  await expect(page.locator(".json-view pre")).toContainText("backup_id");
  expect(
    await page.getByLabel("服务备份 ID", { exact: true }).inputValue(),
  ).toMatch(/^[0-9a-f]{64}$/);
  await page
    .getByRole("button", { name: "恢复 / 重试到新目录", exact: true })
    .click();
  await page.getByRole("button", { name: "确认恢复", exact: true }).click();
  await expect(page.locator(".json-view pre")).toContainText("target_root");
  await expect(page.locator(".json-view pre")).toContainText(
    '"started": false',
  );
  expect(
    await page.evaluate(() => JSON.stringify(sessionStorage)),
  ).not.toContain(process.env.ANYSEEK_TEST_MASTER!);
});

for (const mode of ["Lexical", "Vector", "Hybrid"]) {
  test(`field form creates ${mode} collection without model name`, async ({ page }) => {
    await login(page);
    await page.getByRole("button", { name: "创建集合", exact: true }).first().click();
    const dialog = page.getByRole("dialog");
    const name = `form_${mode.toLowerCase()}`;
    await dialog.getByLabel("集合名称", { exact: true }).fill(name);
    await dialog.getByLabel("字段 1 名称", { exact: true }).fill("custom_title");
    await dialog.getByRole("combobox", { name: "检索模式", exact: true }).selectOption(mode);
    await expect(dialog.getByLabel("字段 1 名称", { exact: true })).toHaveValue("custom_title");
    await expect(dialog.locator("textarea")).toHaveCount(0);
    await expect(dialog.getByText("Embedding 模型标识", { exact: true })).toHaveCount(0);
    if (mode !== "Lexical") await dialog.getByLabel("向量维度", { exact: true }).fill("2");
    const sent = page.waitForRequest((r) => r.method() === "POST" && r.url().endsWith("/api/v1/index"));
    await dialog.getByRole("button", { name: "创建集合", exact: true }).click();
    expect((await sent).postDataJSON()).not.toHaveProperty("embedding_space_id");
    await expect(dialog).toHaveCount(0);
    const info = await call(`index/${name}`);
    expect(info.schema[0].field).toBe("custom_title");
  });
}

test("browser-only model configuration, embedded ingest and natural language vector search", async ({ page }) => {
  const id = "browser_embeddings";
  await call("index", {
    operation_id: operation(), collection_id: id,
    config: { index_name: id, schema: [
      { field: "body", field_type: "Text" },
      { field: "vector", field_type: "Json", index_vector: true },
    ], vector: { dimensions: 3 } },
  });
  expect((await call(`index/${id}`)).vector_dimensions).toBe(3);
  let modelCalls = 0;
  const modelKey = "browser-only-model-secret";
  await page.route("https://model.example.test/v1/embeddings", async (route) => {
    const request = route.request();
    if (request.method() === "OPTIONS") {
      await route.fulfill({ status: 204, headers: { "access-control-allow-origin": "*", "access-control-allow-headers": "content-type,authorization", "access-control-allow-methods": "POST" } });
      return;
    }
    modelCalls++;
    expect(request.headers().authorization).toBe("Bearer " + modelKey);
    expect(request.headers()).not.toHaveProperty("apikey");
    const body = request.postDataJSON();
    expect(body.model).toBe("test-embedding-model");
    await route.fulfill({ contentType: "application/json", headers: { "access-control-allow-origin": "*" }, body: JSON.stringify({
      data: body.input.map((_: string, index: number) => ({ index, embedding: [1, 0, 0] })).reverse(),
    }) });
  });
  page.on("request", (request) => {
    if (new URL(request.url()).pathname.startsWith("/api/v1/")) {
      expect(request.postData() || "").not.toContain(modelKey);
      expect(request.postData() || "").not.toContain("model.example.test");
      expect(request.headers().authorization).toBeUndefined();
    }
  });
  await login(page);
  await page.getByLabel("当前集合", { exact: true }).selectOption(id);
  await page.getByRole("button", { name: "连接与管理", exact: true }).click();
  await page.getByLabel("Embeddings API 地址", { exact: true }).fill("https://model.example.test/v1/embeddings");
  await page.getByLabel("模型名称", { exact: true }).fill("test-embedding-model");
  await page.getByLabel("模型 API Key（可选）", { exact: true }).fill(modelKey);
  await page.getByLabel("在当前标签页会话中记住模型 Key", { exact: true }).check();
  await page.getByRole("button", { name: "测试连接", exact: true }).click();
  await expect(page.getByText("连接成功，返回 3 维向量。请保存配置后使用。", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "保存本地配置", exact: true }).click();
  expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toContain(modelKey);
  await page.reload();
  await expect(page.getByLabel("模型 API Key（可选）", { exact: true })).toHaveValue(modelKey);
  await page.getByRole("navigation", { name: "主导航" }).getByRole("button", { name: "数据", exact: true }).click();
  await page.locator(".page-heading").filter({ has: page.getByRole("heading", { name: "数据", exact: true }) }).getByRole("button", { name: "导入数据", exact: true }).click();
  await expect(page.getByLabel("自动生成缺失的向量", { exact: true })).toBeChecked();
  const source = "[" + Array.from({ length: 11 }, (_, i) => `{"body":"embedding document ${i}","large":9223372036854775807}`).join(",") + "]";
  await page.getByLabel("导入 JSON", { exact: true }).fill(source);
  await page.getByRole("button", { name: "写入集合", exact: true }).click();
  await expect(page.getByText("已确认提交 11 条，索引正在异步更新。", { exact: true })).toBeVisible();
  expect(modelCalls).toBe(2);
  await page.getByRole("dialog").locator(".modal-actions").getByRole("button", { name: "关闭", exact: true }).click();
  await expect.poll(async () => {
    const p = await call(`index/${id}/ingest`);
    return p.vector_seq >= p.committed_seq && p.lexical_seq >= p.committed_seq;
  }).toBe(true);
  await page.getByRole("navigation", { name: "主导航" }).getByRole("button", { name: "检索调试", exact: true }).click();
  await page.getByRole("button", { name: "向量 Vector", exact: true }).click();
  await page.getByLabel("检索内容", { exact: true }).fill("find documents");
  await page.getByRole("button", { name: /筛选与参数/ }).click();
  await page.getByText("高级查询参数 · JSON", { exact: true }).click();
  await page.getByLabel("高级查询参数", { exact: true }).fill('{"all":true}');
  await page.getByRole("button", { name: "运行检索", exact: true }).click();
  await expect(page.locator(".result-card")).toHaveCount(10);
  expect(modelCalls).toBe(3);
  await page.getByRole("button", { name: "下一页结果", exact: true }).click();
  await expect(page.locator(".result-card")).toHaveCount(1);
  expect(modelCalls).toBe(3);
  await page.getByRole("button", { name: "连接与管理", exact: true }).click();
  await page.getByRole("button", { name: "清除本地模型配置", exact: true }).click();
  expect(await page.evaluate(() => Object.keys(localStorage).filter((key) => key.startsWith("anyseek.embedding.")))).toEqual([]);
  expect(await page.evaluate(() => JSON.stringify(sessionStorage))).not.toContain(modelKey);
});
