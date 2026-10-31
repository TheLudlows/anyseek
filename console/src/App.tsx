import { useEffect, useRef, useState } from "react";
import {
  ArrowRight,
  ChevronRight,
  CircleHelp,
  Database,
  ExternalLink,
  Layers3,
  LogOut,
  Menu,
  RefreshCw,
  Search as SearchIcon,
  Settings2,
  ShieldCheck,
} from "lucide-react";
import { api, message, ownerFor, storage } from "./api.ts";
import { Busy, Empty, Modal, Notice } from "./components.tsx";
import { Collections } from "./Collections.tsx";
import { Data } from "./Data.tsx";
import { Search } from "./Search.tsx";
import { Initialize } from "./Initialize.tsx";
import { EmbeddingSettings } from "./EmbeddingSettings.tsx";
import { clearEmbeddingKeys } from "./embeddings.ts";
import { Recovery } from "./Tools.tsx";
import type { Collection } from "./types.ts";

type Section = "collections" | "data" | "search" | "settings";
const sections = [
  { id: "collections", name: "集合", icon: Layers3 },
  { id: "data", name: "数据", icon: Database },
  { id: "search", name: "检索调试", icon: SearchIcon },
] as const;
function saved<T>(key: string, fallback: T): T {
  try {
    return storage.get<T>(key) ?? fallback;
  } catch {
    return fallback;
  }
}
export default function App() {
  const [apiKey, setApiKey] = useState(""),
    [owner, setOwner] = useState("");
  const [collections, setCollections] = useState<Collection[]>([]),
    [current, setCurrent] = useState(() => saved("collection", ""));
  const [section, setSection] = useState<Section>(() =>
    ["data", "search", "settings"].includes(location.hash.slice(1))
      ? (location.hash.slice(1) as Section)
      : "collections",
  );
  const [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [connectOpen, setConnectOpen] = useState(false),
    [mobileOpen, setMobileOpen] = useState(false);
  const sequence = useRef(0),
    currentKey = useRef(""),
    booted = useRef(false);
  const selected = collections.find((c) => c.id === current);
  useEffect(() => {
    const update = () => {
      const hash = location.hash.slice(1);
      if (["collections", "data", "search", "settings"].includes(hash))
        setSection(hash as Section);
    };
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  useEffect(() => {
    if (booted.current) return;
    booted.current = true;
    const key = saved("apiKey", "");
    if (key) void connect(key).catch(() => {});
  }, []);
  function navigate(page: Section) {
    setSection(page);
    location.hash = page;
    setMobileOpen(false);
  }
  async function connect(key: string) {
    const attempt = ++sequence.current;
    setBusy(true);
    setError("");
    try {
      if (!key.trim())
        throw new Error("请输入服务管理员签发的 Tenant API Key。");
      const fingerprint = await ownerFor(key.trim());
      const list = await api<Collection[]>(key.trim(), "apikey");
      if (attempt !== sequence.current) return;
      storage.set("apiKey", key.trim());
      const changed = currentKey.current && currentKey.current !== key.trim();
      if (changed) clearEmbeddingKeys(owner);
      currentKey.current = key.trim();
      setApiKey(key.trim());
      setOwner(fingerprint);
      setCollections(list);
      setConnectOpen(false);
      const next =
        !changed && list.some((c) => c.id === current)
          ? current
          : list[0]?.id || "";
      setCurrent(next);
      storage.set("collection", next);
      if (changed) navigate("collections");
    } catch (e) {
      if (attempt === sequence.current) {
        setError(message(e));
        throw e;
      }
    } finally {
      if (attempt === sequence.current) setBusy(false);
    }
  }
  async function refresh() {
    const key = currentKey.current,
      attempt = ++sequence.current;
    setBusy(true);
    setError("");
    try {
      const list = await api<Collection[]>(key, "apikey");
      if (currentKey.current !== key || attempt !== sequence.current) return;
      setCollections(list);
      setCurrent((id) =>
        list.some((c) => c.id === id) ? id : list[0]?.id || "",
      );
    } catch (e) {
      if (attempt === sequence.current) setError(message(e));
    } finally {
      if (attempt === sequence.current) setBusy(false);
    }
  }
  function select(id: string, page: "data" | "search" = "data") {
    setCurrent(id);
    storage.set("collection", id);
    navigate(page);
  }
  function disconnect() {
    clearEmbeddingKeys(owner);
    ++sequence.current;
    currentKey.current = "";
    setApiKey("");
    setOwner("");
    setCollections([]);
    setCurrent("");
    setError("");
    setBusy(false);
    setConnectOpen(false);
    storage.remove("apiKey");
    storage.remove("collection");
    navigate("collections");
  }
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">
        跳到主要内容
      </a>
      {mobileOpen && (
        <button
          className="sidebar-scrim"
          aria-label="关闭导航"
          onClick={() => setMobileOpen(false)}
        />
      )}
      <aside className={"sidebar " + (mobileOpen ? "mobile-open" : "")}>
        <a
          className="brand"
          href="#collections"
          onClick={() => navigate("collections")}
        >
          <img src="/favicon.svg" alt="" />
          <span>
            AnySeek<span className="brand-dot">.</span>
          </span>
        </a>
        <span className="nav-label">工作空间</span>
        <nav aria-label="主导航">
          {sections.map((s) => (
            <button
              key={s.id}
              className={section === s.id ? "active" : ""}
              onClick={() => navigate(s.id)}
            >
              <s.icon size={18} />
              <span>{s.name}</span>
              {s.id === "collections" && apiKey && (
                <span className="nav-count">{collections.length}</span>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="sidebar-guide">
            <div className="guide-icon">
              <Layers3 size={17} />
            </div>
            <strong>管理 AI 记忆与知识</strong>
            <p>
              从原始记录到精准检索，
              <br />
              在同一工作区完成。
            </p>
            <a
              href="https://github.com/TheLudlows/anyseek/blob/master/console/README.md"
              target="_blank"
              rel="noreferrer"
            >
              阅读控制台指南
              <ExternalLink size={12} />
            </a>
          </div>
          <button
            className={
              "settings-link " + (section === "settings" ? "active" : "")
            }
            onClick={() => navigate("settings")}
          >
            <Settings2 size={17} />
            连接与管理
          </button>
          <button
            className="connection-card"
            onClick={() => setConnectOpen(true)}
          >
            <span className={"connection-orb " + (apiKey ? "connected" : "")}>
              <ShieldCheck size={17} />
            </span>
            <span>
              <strong>{apiKey ? "服务已连接" : "连接 AnySeek 服务"}</strong>
              <small>{apiKey ? "Tenant API" : "使用 Tenant API Key"}</small>
            </span>
            <ChevronRight size={14} />
          </button>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            <button
              className="icon-button mobile-toggle"
              aria-label="打开导航"
              onClick={() => setMobileOpen(true)}
            >
              <Menu size={19} />
            </button>
            <span>工作空间</span>
            <ChevronRight size={13} />
            <strong>
              {section === "settings"
                ? "连接与管理"
                : sections.find((s) => s.id === section)?.name}
            </strong>
          </div>
          <div className="topbar-actions">
            {apiKey && (
              <>
                <label className="collection-select">
                  <Layers3 size={14} />
                  <span className="sr-only">当前集合</span>
                  <select
                    aria-label="当前集合"
                    value={selected?.id || ""}
                    onChange={(e) => {
                      setCurrent(e.target.value);
                      storage.set("collection", e.target.value);
                    }}
                  >
                    <option value="" disabled>
                      选择集合
                    </option>
                    {collections.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.name}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className="icon-button"
                  aria-label="刷新集合统计"
                  title="刷新集合统计"
                  disabled={busy}
                  onClick={refresh}
                >
                  <RefreshCw size={16} className={busy ? "spin" : ""} />
                </button>
                <span className="topbar-divider" />
              </>
            )}
            <a
              className="icon-button"
              aria-label="打开服务文档"
              href="https://github.com/TheLudlows/anyseek/blob/master/anyseek/src/server/README.md"
              target="_blank"
              rel="noreferrer"
            >
              <CircleHelp size={18} />
            </a>
            <span className="user-avatar" title="AnySeek 工作区">
              A
            </span>
          </div>
        </header>
        <main id="main-content">
          <Notice>{error}</Notice>
          {section === "settings" ? (
            <>
              <div className="page-heading">
                <div>
                  <span className="eyebrow">CONNECTION & ADMINISTRATION</span>
                  <h1>连接与管理</h1>
                  <p>管理当前浏览器标签页的连接，以及服务备份。</p>
                </div>
              </div>
              <section className="panel connection-panel">
                <div>
                  <span className="eyebrow">TENANT CONNECTION</span>
                  <h2>{apiKey ? "服务已连接" : "尚未连接"}</h2>
                  <p>
                    API Key 仅保存在当前标签页的会话中；写入重试记录单独保留。
                  </p>
                </div>
                <div className="inline-actions">
                  <button
                    className="secondary"
                    onClick={() => setConnectOpen(true)}
                  >
                    切换连接
                  </button>
                  {apiKey && (
                    <button className="text-button" onClick={disconnect}>
                      <LogOut size={14} />
                      断开连接
                    </button>
                  )}
                </div>
              </section>
              {apiKey && selected?.schema.some((f) => f.index_vector) ? (
                <EmbeddingSettings key={owner + selected.id} owner={owner} collection={selected} />
              ) : apiKey && <section className="panel embedding-panel"><h2>浏览器嵌入</h2><p className="muted">在左侧选择一个含向量字段的集合，配置仅保存在浏览器的模型连接。</p></section>}
              <Recovery />
            </>
          ) : !apiKey ? (
            <Welcome busy={busy} connect={connect} />
          ) : section === "collections" ? (
            <Collections
              collections={collections}
              apiKey={apiKey}
              owner={owner}
              select={select}
              refresh={refresh}
            />
          ) : !selected ? (
            <section className="panel">
              <Empty
                title="先选择一个集合"
                text="创建集合并定义字段后，即可导入与检索数据。"
                action={
                  <button
                    className="primary"
                    onClick={() => navigate("collections")}
                  >
                    前往集合
                    <ArrowRight size={15} />
                  </button>
                }
              />
            </section>
          ) : section === "data" ? (
            <Data
              key={owner + selected.id}
              collection={selected}
              apiKey={apiKey}
              owner={owner}
              onWritten={refresh}
            />
          ) : (
            <Search
              key={owner + selected.id}
              collection={selected}
              apiKey={apiKey}
              owner={owner}
            />
          )}
          <footer className="main-footer">
            <span>
              AnySeek<span className="footer-dot">·</span>AI 记忆与知识检索
            </span>
            <span>
              {selected?.version ? "Engine v" + selected.version : "检索控制台"}
              <span className="footer-dot">·</span>Console 1.0
            </span>
          </footer>
        </main>
      </div>
      {connectOpen && (
        <Modal
          title="连接 AnySeek"
          close={() => setConnectOpen(false)}
          busy={busy}
        >
          <p className="modal-intro">
            使用管理员签发的 Tenant API Key 访问集合。
          </p>
          <ConnectForm busy={busy} connect={connect} />
          <Notice>{error}</Notice>
        </Modal>
      )}
    </div>
  );
}
function ConnectForm({
  connect,
  busy,
}: {
  connect: (key: string) => Promise<void>;
  busy: boolean;
}) {
  const [initialize, setInitialize] = useState(false);
  const [key, setKey] = useState(""),
    [error, setError] = useState("");
  if (initialize)
    return <Initialize connect={connect} back={() => setInitialize(false)} />;
  return (
    <form
      className="connect-form"
      onSubmit={async (e) => {
        e.preventDefault();
        setError("");
        try {
          await connect(key);
        } catch (e) {
          setError(message(e));
        }
      }}
    >
      <label>
        Tenant API Key
        <input
          type="password"
          autoComplete="off"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          placeholder="粘贴你的 Tenant API Key"
          required
          disabled={busy}
        />
      </label>
      <Notice>{error}</Notice>
      <button className="primary" disabled={busy}>
        {busy ? (
          <Busy label="连接中" />
        ) : (
          <>
            连接服务
            <ArrowRight size={15} />
          </>
        )}
      </button>
      <button
        type="button"
        className="text-button"
        disabled={busy}
        onClick={() => setInitialize(true)}
      >
        首次使用？使用 Master Key 初始化
      </button>
    </form>
  );
}
function Welcome(props: {
  connect: (key: string) => Promise<void>;
  busy: boolean;
}) {
  return (
    <div className="welcome-grid">
      <div className="welcome-copy">
        <span className="eyebrow">WELCOME TO ANYSEEK</span>
        <h1>
          AI 记忆与知识的
          <br />
          检索底座
        </h1>
        <p>统一管理记录、向量与关系，通过全文、向量和混合检索访问你的数据。</p>
        <div className="welcome-features">
          <span>
            <Layers3 size={16} />
            统一集合管理
          </span>
          <span>
            <Database size={16} />
            完整数据表达
          </span>
          <span>
            <SearchIcon size={16} />
            多路检索调试
          </span>
        </div>
        <div className="welcome-art" aria-hidden="true">
          <div className="art-lines" />
          <div className="art-box art-source">
            <Database size={22} />
            <span>DATA</span>
          </div>
          <div className="art-box art-search">
            <SearchIcon size={22} />
            <span>SEARCH</span>
          </div>
          <span className="art-caption">Records. Vectors. Relations.</span>
        </div>
      </div>
      <section className="panel welcome-connect">
        <span className="connect-emblem">
          <ShieldCheck size={23} />
        </span>
        <h2>连接 AnySeek 服务</h2>
        <p>连接你的 AnySeek 服务，进入检索工作区。</p>
        <ConnectForm {...props} />
        <div className="welcome-secure">
          <ShieldCheck size={13} />
          密钥仅用于当前会话
        </div>
      </section>
    </div>
  );
}
