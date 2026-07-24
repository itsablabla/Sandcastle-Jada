/**
 * MCP Bridge connector - call any Model Context Protocol server from workflows.
 * Dual-mode: importable functions + CLI dispatch.
 *
 * Credentials:
 * - TOOL_MCP_SERVER_URL: HTTP(S) endpoint of the MCP server (required)
 * - TOOL_MCP_AUTH_TOKEN: Bearer token (optional)
 *
 * Speaks streamable HTTP MCP: JSON-RPC over POST, SSE or JSON responses,
 * Mcp-Session-Id session affinity, and notifications/initialized after initialize.
 */

const SERVER_URL = process.env.TOOL_MCP_SERVER_URL || "";
const AUTH_TOKEN = process.env.TOOL_MCP_AUTH_TOKEN || "";
let _requestId = 0;
let _sessionId = null;
let _initialized = false;

function nextId() {
  return ++_requestId;
}

function buildHeaders() {
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
  };
  if (AUTH_TOKEN) {
    headers.Authorization = `Bearer ${AUTH_TOKEN}`;
  }
  if (_sessionId) {
    headers["Mcp-Session-Id"] = _sessionId;
  }
  return headers;
}

function parseRpcPayload(text, contentType = "") {
  const trimmed = (text || "").trim();
  if (!trimmed) {
    throw new Error("Empty MCP response body");
  }

  const ct = (contentType || "").toLowerCase();
  const contentTypeIsSse = ct.includes("text/event-stream");
  // SSE frames are line-oriented (optional "event:" then "data:"). A JSON body
  // that merely contains the substring "data:" in a string value is NOT SSE.
  const firstLine = trimmed.split(/\r?\n/, 1)[0] || "";
  const startsLikeSse =
    firstLine.startsWith("data:") || firstLine.startsWith("event:");

  const tryJson = () => {
    try {
      return JSON.parse(trimmed);
    } catch {
      return null;
    }
  };

  // Prefer JSON whenever the body is a JSON value, unless the server explicitly
  // labeled the response as SSE (streamable HTTP often still sends JSON-RPC in
  // data: lines with content-type text/event-stream).
  if (!contentTypeIsSse && (trimmed.startsWith("{") || trimmed.startsWith("["))) {
    const parsed = tryJson();
    if (parsed !== null) {
      return parsed;
    }
  }

  if (contentTypeIsSse || startsLikeSse) {
    const dataLines = trimmed
      .split(/\r?\n/)
      .map((l) => l.trimEnd())
      .filter((l) => l.startsWith("data:"))
      .map((l) => l.slice(5).trim())
      .filter(Boolean);
    if (dataLines.length === 0) {
      throw new Error("SSE MCP response had no data lines");
    }
    // Prefer the last JSON-RPC message that carries result/error
    let last = null;
    for (const line of dataLines) {
      if (line === "[DONE]") continue;
      try {
        last = JSON.parse(line);
      } catch {
        throw new Error(`Invalid SSE JSON from MCP server: ${line.slice(0, 200)}`);
      }
    }
    if (!last) {
      throw new Error("SSE MCP response had no JSON payload");
    }
    return last;
  }

  // Plain JSON body (or last-resort parse)
  const parsed = tryJson();
  if (parsed !== null) {
    return parsed;
  }
  throw new Error(`Invalid JSON from MCP server: ${trimmed.slice(0, 200)}`);
}

async function postRaw(body, { notification = false } = {}) {
  if (!SERVER_URL) {
    throw new Error("TOOL_MCP_SERVER_URL is not set");
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120000);
  let resp;
  try {
    resp = await fetch(SERVER_URL, {
      method: "POST",
      headers: buildHeaders(),
      body: JSON.stringify(body),
      signal: controller.signal,
    });
  } catch (err) {
    clearTimeout(timer);
    if (err && err.name === "AbortError") {
      throw new Error(`MCP server timeout after 120s: ${SERVER_URL}`);
    }
    throw err;
  }
  clearTimeout(timer);

  const newSessionId = resp.headers.get("mcp-session-id");
  if (newSessionId) {
    _sessionId = newSessionId;
  }

  // Notifications may return 202/204 with empty body
  if (notification) {
    if (!resp.ok && resp.status !== 202 && resp.status !== 204) {
      const text = await resp.text();
      throw new Error(`MCP server ${resp.status}: ${text.slice(0, 500)}`);
    }
    return null;
  }

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`MCP server ${resp.status}: ${text.slice(0, 500)}`);
  }
  return parseRpcPayload(text, resp.headers.get("content-type") || "");
}

async function rpc(method, params = {}) {
  const body = {
    jsonrpc: "2.0",
    id: nextId(),
    method,
    params,
  };
  const data = await postRaw(body);
  if (data.error) {
    const msg =
      typeof data.error.message === "string"
        ? data.error.message
        : JSON.stringify(data.error.message ?? data.error);
    throw new Error(`MCP error ${data.error.code}: ${msg}`);
  }
  return data.result;
}

async function ensureInitialized() {
  if (_initialized && _sessionId) {
    return;
  }
  await rpc("initialize", {
    protocolVersion: "2024-11-05",
    capabilities: {},
    clientInfo: { name: "sandcastle-mcp-bridge", version: "1.1.0" },
  });
  // Streamable HTTP servers expect the initialized notification before other calls
  try {
    await postRaw(
      { jsonrpc: "2.0", method: "notifications/initialized" },
      { notification: true },
    );
  } catch {
    // Some servers ignore notifications; tools/list will fail clearly if required
  }
  _initialized = true;
}

export async function list_tools() {
  await ensureInitialized();
  const result = await rpc("tools/list");
  return (result.tools || []).map((t) => ({
    name: t.name,
    description: t.description || "",
    inputSchema: t.inputSchema || {},
  }));
}

export async function call_tool(toolName, args = "{}") {
  if (!toolName || typeof toolName !== "string") {
    throw new Error(
      "call_tool requires toolName as argv[3] (got " +
        String(toolName) +
        "). Workflow YAML must set tool_config.arguments: [toolName, jsonArgs]",
    );
  }
  let parsedArgs = args;
  if (typeof args === "string") {
    try {
      parsedArgs = args.trim() === "" ? {} : JSON.parse(args);
    } catch (err) {
      throw new Error(`call_tool arguments must be JSON: ${err.message}`);
    }
  }
  if (parsedArgs == null || typeof parsedArgs !== "object" || Array.isArray(parsedArgs)) {
    throw new Error("call_tool arguments must be a JSON object");
  }
  await ensureInitialized();
  const result = await rpc("tools/call", {
    name: toolName,
    arguments: parsedArgs,
  });
  const content = result.content || [];
  const texts = content.filter((c) => c.type === "text").map((c) => c.text);
  const images = content
    .filter((c) => c.type === "image")
    .map((c) => ({ mimeType: c.mimeType, data: c.data?.slice(0, 200) + "..." }));
  return {
    tool: toolName,
    isError: result.isError || false,
    text: texts.join("\n"),
    images: images.length > 0 ? images : undefined,
    rawContentCount: content.length,
  };
}

export async function list_resources() {
  await ensureInitialized();
  const result = await rpc("resources/list");
  return (result.resources || []).map((r) => ({
    uri: r.uri,
    name: r.name || "",
    description: r.description || "",
    mimeType: r.mimeType || "text/plain",
  }));
}

export async function read_resource(uri) {
  if (!uri) throw new Error("uri is required");
  await ensureInitialized();
  const result = await rpc("resources/read", { uri });
  const contents = result.contents || [];
  return contents.map((c) => ({
    uri: c.uri,
    mimeType: c.mimeType || "text/plain",
    text: c.text ? c.text.slice(0, 50000) : undefined,
    blob: c.blob ? c.blob.slice(0, 200) + "..." : undefined,
  }));
}

// CLI dispatch - stdout must be ONLY the JSON result (executor parses it)
if (process.argv[1]?.endsWith("mcp-bridge.mjs")) {
  const [fn, ...args] = process.argv.slice(2);
  const dispatch = { list_tools, call_tool, list_resources, read_resource };
  if (!dispatch[fn]) {
    console.error(
      "Usage: node mcp-bridge.mjs <list_tools|call_tool|list_resources|read_resource> [args...]",
    );
    process.exit(1);
  }
  try {
    const result = await dispatch[fn](...args);
    console.log(JSON.stringify(result, null, 2));
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}
