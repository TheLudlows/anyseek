import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { createHash, randomBytes } from "node:crypto";

export default async function setup() {
  const root = await mkdtemp(join(tmpdir(), "anyseek-console-"));
  const secret = randomBytes(32).toString("hex");
  const master = createHash("sha256").update(secret).digest("base64");
  const key = randomBytes(32).toString("base64");
  const processEnv: NodeJS.ProcessEnv = {
    ...process.env,
    MASTER_KEY_SECRET: secret,
  };
  // These browser checks always use local disposable storage, never a configured S3 bucket.
  for (const name of Object.keys(processEnv))
    if (name.startsWith("ANYSEEK_S3_") || name === "ANYSEEK_STORAGE_BACKEND")
      delete processEnv[name];
  const binary =
    process.env.ANYSEEK_SERVER ||
    resolve(
      "../target/debug/anyseek_server" +
        (process.platform === "win32" ? ".exe" : ""),
    );
  const server = spawn(
    binary,
    [
      "index_path=" + join(root, "data"),
      "local_port=18180",
      "local_ip=127.0.0.1",
    ],
    { env: processEnv, stdio: "ignore" },
  );
  let failure: Error | null = null;
  server.on("error", (error) => {
    failure = error;
  });
  const stop = async () => {
    if (server.exitCode === null) {
      server.kill();
      await new Promise<void>((done) => {
        server.once("exit", () => done());
        setTimeout(() => {
          server.kill("SIGKILL");
          done();
        }, 5000).unref();
      });
    }
    await rm(root, { recursive: true, force: true });
  };
  try {
    let ready = false;
    for (let i = 0; i < 200; i++) {
      if (failure) throw failure;
      if (server.exitCode !== null)
        throw new Error("Disposable API server exited: " + server.exitCode);
      try {
        if ((await fetch("http://127.0.0.1:18180/api/v1/live")).ok) {
          ready = true;
          break;
        }
      } catch {}
      await new Promise((done) => setTimeout(done, 100));
    }
    if (!ready) throw new Error("Disposable API server readiness timeout");
    const operation = () => ({
      epoch: Math.floor(Date.now() / 86400000),
      nonce: randomBytes(16).toString("hex"),
    });
    const post = async (path: string, body: object, auth = key) => {
      const response = await fetch("http://127.0.0.1:18180/api/v1/" + path, {
        method: "POST",
        headers: { apikey: auth, "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok)
        throw new Error("Fixture API " + path + ": " + (await response.text()));
      return response.json();
    };
    await post(
      "tenants",
      { operation_id: operation(), tenant_id: "console_test" },
      master,
    );
    await post(
      "apikey",
      {
        operation_id: operation(),
        tenant_id: "console_test",
        key_id: "console_key",
        apikey_base64: key,
      },
      master,
    );
    for (const [id, mode] of [
      ["knowledge_base", "Hybrid"],
      ["product_docs", "Lexical"],
      ["support_cases", "Vector"],
    ]) {
      const schema = [
        {
          field: "title",
          field_type: "Text",
          ...(mode === "Vector" ? { index_lexical: false } : {}),
        },
        {
          field: "body",
          field_type: "Text",
          ...(mode === "Vector" ? { index_lexical: false } : {}),
        },
        { field: "url", field_type: "Text", index_lexical: false },
        ...(mode === "Lexical"
          ? []
          : [{ field: "vector", field_type: "Json", index_vector: true }]),
      ];
      await post("index", {
        operation_id: operation(),
        collection_id: id,
        config: {
          index_name: id,
          schema,
          ...(mode === "Lexical" ? {} : { vector: { dimensions: 3 } }),
        },
        ...(mode === "Lexical"
          ? {}
          : { embedding_space_id: "console-test-v1" }),
      });
    }
    const documents = [
      [
        "混合检索：连接关键词与语义",
        "混合检索结合关键词匹配与向量相似度，通过融合排序覆盖不同的信息需求。",
      ],
      [
        "面向 AI 记忆的数据组织",
        "使用 Collection 组织原始记录、来源证据与显式关系，让检索结果可以追溯。",
      ],
      [
        "理解检索通道与结果得分",
        "在检索调试中查看每条记录的通道与原始得分，分析查询效果。",
      ],
      [
        "从原始记录开始构建知识库",
        "保留原始信息与结构化字段，为关键词、向量及混合检索提供基础。",
      ],
    ];
    for (let i = 0; i < documents.length; i++)
      await post("index/knowledge_base/doc", {
        operation_id: operation(),
        record_id: "guide_" + (i + 1),
        expected: "Absent",
        document: {
          title: documents[i][0],
          body: documents[i][1],
          vector: [1, 0, i / 10],
          url: "https://example.com/guide/" + (i + 1),
        },
      });
    process.env.ANYSEEK_TEST_KEY = key;
    process.env.ANYSEEK_TEST_MASTER = master;
    return stop;
  } catch (error) {
    await stop();
    throw error;
  }
}
