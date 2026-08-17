// Makes the docs LLM/agent-friendly.
//
// Served URLs (dev via a webpack-dev-server middleware, prod via postBuild):
//   <baseUrl>/docs/<doc>.md   -> raw markdown source of a docs page
//   <baseUrl>/llms-full.txt   -> every doc concatenated (sidebar order)
//
// Discoverability added on top:
//   - postBuild injects <link rel="alternate" type="text/markdown" href="...">
//     into each docs page's <head>, so an agent fetching a page's HTML can find
//     that page's markdown twin.
//   - static/llms.txt (a curated index) is a plain static file, not handled here.
//
// The .md URLs are route-based (they mirror the page URL, using Docusaurus's
// default docs route generation):
//   docs/intro.md          -> /docs/intro.md
//   docs/evals/index.md    -> /docs/evals.md
//   docs/evals/metrics.md  -> /docs/evals/metrics.md

const fs = require("fs");
const path = require("path");

const DOC_EXTENSIONS = [".md", ".mdx"];

// Reading order for llms-full.txt (mirrors sidebars.js). Any doc not listed
// here is appended at the end (sorted), so newly added docs never disappear.
const FULL_ORDER = [
  "intro",
  "tutorial/getting-started",
  "configuration",
  "datasources",
  "check-types/index",
  "check-types/row-count",
  "check-types/numeric",
  "check-types/sum",
  "check-types/min",
  "check-types/max",
  "check-types/measure",
  "check-types/not-empty",
  "check-types/not-empty-pct",
  "check-types/anomaly",
  "evals/index",
  "evals/configuration",
  "evals/commands",
  "evals/metrics",
];

function posix(p) {
  return p.split(path.sep).join("/");
}

function stripTrailingSlash(p) {
  return p.replace(/\/+$/, "");
}

/** Recursively collect files under `dir` whose extension is in `exts`. */
function collectFiles(dir, exts) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  const stack = [dir];
  while (stack.length > 0) {
    const current = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch (_err) {
      continue;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else if (entry.isFile() && exts.includes(path.extname(entry.name))) {
        out.push(full);
      }
    }
  }
  return out;
}

module.exports = function serveMarkdownPlugin(context, options = {}) {
  const { siteDir, baseUrl } = context;
  const siteUrl = (context.siteConfig && context.siteConfig.url) || "";
  const docsDir = options.docsDir || "docs";
  const routeBasePath = options.routeBasePath || "/docs";
  const docsAbsDir = path.resolve(siteDir, docsDir);
  const base = stripTrailingSlash(baseUrl || "/");
  const routeBase = stripTrailingSlash(routeBasePath);
  const absBase = stripTrailingSlash(siteUrl);
  const llmsFullPath = `${base}/llms-full.txt`;

  /**
   * Docusaurus default docs route path for a doc, given its extension-less
   * path relative to the docs dir. Index files collapse into their folder.
   */
  function routePathFor(relNoExt) {
    let rel = relNoExt;
    if (/^index$/.test(rel) || /\/index$/.test(rel)) {
      rel = rel.replace(/\/?index$/, "");
    }
    if (rel === "") return `${base}${routeBase}`;
    return `${base}${routeBase}/${rel}`;
  }

  /**
   * Rebuilt on demand so newly added docs are always picked up.
   * Returns:
   *   byMdUrl: Map  mdUrl  -> absolute source file
   *   byRel:   Map  relNoExt -> { file, mdUrl }
   */
  function buildIndex() {
    const byMdUrl = new Map();
    const byRel = new Map();
    for (const file of collectFiles(docsAbsDir, DOC_EXTENSIONS)) {
      const rel = posix(path.relative(docsAbsDir, file)); // "evals/metrics.md"
      const relNoExt = rel.replace(/\.(md|mdx)$/, ""); // "evals/metrics"
      const mdUrl = `${routePathFor(relNoExt)}.md`;
      byMdUrl.set(mdUrl, file);
      byRel.set(relNoExt, { file, mdUrl });
    }
    return { byMdUrl, byRel };
  }

  function resolveMarkdown(urlPath) {
    const file = buildIndex().byMdUrl.get(urlPath);
    if (!file || !fs.existsSync(file)) return null;
    return fs.readFileSync(file, "utf8");
  }

  /** Docs in reading order (FULL_ORDER first, then any unlisted, sorted). */
  function orderedDocs() {
    const { byRel } = buildIndex();
    const seen = new Set();
    const docs = [];
    for (const rel of FULL_ORDER) {
      const d = byRel.get(rel);
      if (d) {
        docs.push(d);
        seen.add(rel);
      }
    }
    for (const rel of [...byRel.keys()].sort()) {
      if (!seen.has(rel)) docs.push(byRel.get(rel));
    }
    return docs;
  }

  function buildLlmsFull() {
    const bar = "=".repeat(72);
    const parts = [
      "# Weiser — Full Documentation",
      "",
      "> Data Quality and Agent Evals in One Framework.",
      "",
      "Complete Weiser documentation for LLM/agent consumption. Each document",
      "is prefixed by its canonical markdown URL.",
      "",
    ];
    for (const d of orderedDocs()) {
      const content = fs.readFileSync(d.file, "utf8").trim();
      parts.push(bar, `URL: ${absBase}${d.mdUrl}`, bar, "", content, "");
    }
    return parts.join("\n");
  }

  /**
   * Inject <link rel="alternate" type="text/markdown"> into a built HTML page,
   * derived from its canonical URL. No-op for pages without a .md twin.
   */
  function injectAlternateLink(htmlFile, byMdUrl) {
    let html = fs.readFileSync(htmlFile, "utf8");
    if (html.includes('type="text/markdown"')) return; // idempotent
    const canonTag = html.match(/<link\b[^>]*\brel="canonical"[^>]*>/);
    if (!canonTag) return;
    const hrefMatch = canonTag[0].match(/href="([^"]+)"/);
    if (!hrefMatch) return;
    let pathname;
    try {
      pathname = new URL(hrefMatch[1]).pathname;
    } catch (_err) {
      return;
    }
    const mdUrl = `${stripTrailingSlash(pathname)}.md`;
    if (!byMdUrl.has(mdUrl)) return;
    const tag = `<link rel="alternate" type="text/markdown" href="${mdUrl}">`;
    html = html.replace(/<\/head>/, `${tag}\n</head>`);
    fs.writeFileSync(htmlFile, html, "utf8");
  }

  return {
    name: "docusaurus-plugin-serve-markdown",

    configureWebpack() {
      return {
        devServer: {
          setupMiddlewares(middlewares) {
            middlewares.unshift({
              name: "serve-docs-markdown",
              middleware(req, res, next) {
                if (req.method !== "GET" && req.method !== "HEAD") {
                  return next();
                }
                const urlPath = decodeURIComponent(
                  (req.url || "").split("?")[0].split("#")[0]
                );

                // /llms-full.txt — full documentation dump
                if (urlPath === llmsFullPath) {
                  res.setHeader("Content-Type", "text/plain; charset=utf-8");
                  res.setHeader("Cache-Control", "no-cache");
                  if (req.method === "HEAD") return res.end();
                  return res.end(buildLlmsFull());
                }

                // /docs/... .md — raw markdown
                if (urlPath.endsWith(".md")) {
                  const body = resolveMarkdown(urlPath);
                  if (body != null) {
                    res.setHeader(
                      "Content-Type",
                      "text/markdown; charset=utf-8"
                    );
                    res.setHeader("Cache-Control", "no-cache");
                    if (req.method === "HEAD") return res.end();
                    return res.end(body);
                  }
                  // Unknown doc under the docs route -> clean 404 (LLM-friendly).
                  // Other `.md` paths (e.g. static files) fall through.
                  const docsPrefix = `${base}${routeBase}/`;
                  if (urlPath.startsWith(docsPrefix)) {
                    res.statusCode = 404;
                    res.setHeader(
                      "Content-Type",
                      "text/plain; charset=utf-8"
                    );
                    return res.end(`404: no markdown doc at ${urlPath}`);
                  }
                }
                return next();
              },
            });
            return middlewares;
          },
        },
      };
    },

    postBuild({ outDir }) {
      const { byMdUrl } = buildIndex();

      // 1) Copy raw .md files to their route-based URLs.
      for (const [mdUrl, file] of byMdUrl) {
        const dest = path.join(outDir, mdUrl.replace(/^\//, ""));
        fs.mkdirSync(path.dirname(dest), { recursive: true });
        fs.copyFileSync(file, dest);
      }

      // 2) Write the full documentation dump.
      const fullDest = path.join(outDir, llmsFullPath.replace(/^\//, ""));
      fs.mkdirSync(path.dirname(fullDest), { recursive: true });
      fs.writeFileSync(fullDest, buildLlmsFull(), "utf8");

      // 3) Add the per-page markdown alternate link to every docs page.
      for (const htmlFile of collectFiles(outDir, [".html"])) {
        injectAlternateLink(htmlFile, byMdUrl);
      }
    },
  };
};
