# Layout / Theme Spec — single source of truth

These rules are **non-negotiable** and override any generic frontend-design choice. They were learned the hard way over several rounds of user feedback. The bundled `example-deck.html` already implements all of this — prefer cloning it and swapping content over re-deriving the CSS.

> Output language: deck **body text is Korean**; **code comments are English**. No external fonts/CDNs (system fonts only).

---

## Format

- A slide **deck**: click / `→` / `Space` = next, `←` = prev, `#` = jump to TOC.
- When a slide has several diffs, split it into **steps** (one visible at a time).
- Two-column **split**: left = code / diff / pseudocode, right = explanation / table / callout.
- ASCII-art matrices/tables are **forbidden inside code** → render them as HTML grid/table components.
- PC only. **1440 is a design reference width, NOT a fixed canvas.** Shrinking the window or zooming the browser must never clip content — overflow is handled by **internal panel scroll** (see below).

## Theme — VS Code Dark Modern

```
--bg-side:#181818;  --bg-editor:#1F1F1F;  --bg-tab:#252526;  --border:#2B2B2B;
--fg:#CCCCCC;       --muted:#858585;
--kw:#569CD6 (keyword blue);  --ctrl:#C586C0 (control purple);
--type:#4EC9B0;  --string:#CE9178;  --num:#B5CEA8;  --comment:#6A9955;  --func:#DCDCAA;  --var:#9CDCFE;
--add:#6A9955;  --del:#F14C4C;  (orange tab stripe ~ #E0913C)
--ui:  -apple-system,"Segoe UI","Apple SD Gothic Neo",system stack
--mono:"SF Mono",Menlo,Monaco,Consolas
```

- Code panel looks like a VS Code editor tab: a thin **orange 1px stripe** on top + the **file path as a right-aligned breadcrumb**.

---

## LAYOUT / SCROLL SKELETON — start from exactly this

Browser zoom is the user enlarging text to read. Therefore:
**(a)** never `transform:scale` the whole deck (text would shrink — the opposite of intent);
**(b)** never clip content;
**(c)** when something doesn't fit, scroll **inside that panel** (like a VS Code editor pane).

```css
html,body { height:100%; margin:0; overflow:hidden; }      /* the page/window itself never scrolls */
#deck { position:fixed; inset:0; overflow:hidden; }        /* fills the viewport; gradient lives here, one layer */
.slide { position:absolute; inset:0; display:flex; flex-direction:column;
         align-items:center; padding:40px 56px 0; }        /* a slide == the viewport */
.head { flex:0 0 auto; width:100%; max-width:1328px; margin:0 auto; } /* fixed header, never scrolls, width-capped */
.body { flex:1 1 auto; min-height:0; width:100%; max-width:1328px; margin:0 auto;
        display:flex; flex-direction:column; overflow:hidden; padding-bottom:60px; }
        /* fills remaining height but does NOT scroll itself — scrolling happens inside panels */
```

Internal-scroll panels (this is what makes zoom work):

```css
.split { display:grid; grid-template-columns:1.16fr .84fr; gap:26px;
         width:100%; height:100%; grid-template-rows:minmax(0,1fr); align-items:stretch; }
.col-left  { min-width:0; min-height:0; display:flex; flex-direction:column; }
.codepanel { flex:1 1 auto; min-height:0; display:flex; flex-direction:column; }
.code-wrap { flex:1 1 auto; min-height:0; overflow:auto; }   /* ★ the CODE BLOCK scrolls inside itself (both axes) */
.col-right { min-width:0; min-height:0; overflow:auto; }      /* right notes scroll internally when tall */
.single    { height:100%; overflow:auto; }                   /* one-column slides (TOC / flow / table) */
```

- **★★ `min-height:0` on EVERY flex/grid ancestor of a scroll container.** This is the #1 gotcha: without it the child can't shrink, so it clips/overflows instead of scrolling.

Footer + progress bar — **always visible at any zoom**:

```css
#footer { position:fixed; left:0; right:0; bottom:0;  height:46px; z-index:50; }  /* full viewport width */
#pbar   { position:fixed; left:0; right:0; bottom:46px; z-index:51; }
```

- Put them as **direct `<body>` children** (no transformed ancestor). Give `.body` a `padding-bottom` so panel bottoms clear the footer; lift the step-dots above it (`bottom:54px+`).
- **Forbidden:** `width:1440 + left:50% + translateX(-50%)` (clips the right end "X/N" when zoomed). **Forbidden:** placing the footer `position:absolute` inside the scrolling area (it scrolls away).

Background gradient — no seam, no banding:

```css
#deck { background:var(--bg-editor);
  background-image:
    radial-gradient(1300px 900px at 82% -8%, rgba(86,156,214,.07), rgba(86,156,214,0) 62%),
    radial-gradient(1100px 760px at 6% 108%, rgba(197,134,192,.06), rgba(197,134,192,0) 58%); }
```

- One continuous layer over the whole viewport. **Fade with `rgba(color,0)`, NEVER the `transparent` keyword** — `transparent` == transparent *black*, so the midpoint goes grey and you get a visible band / hard cut.

---

## Must-have details (commonly missed)

- **Diff `+/-` rows: no blank line between them.** `pre{white-space:pre}` would render the source `\n` between `<span class="ln">` as extra blank lines. Use `.code-wrap pre { white-space:normal }` + `.ln { white-space:pre; display:block }`, and build line spans with no literal newline between them (join in JS).
- **Korean body text, English code comments.** The author's Korean annotations in the source md belong in the right column; keep code comments English/authentic.
- Inline code = light-blue pill (`<span class="inl">` / `<code>`).
- Tables = VS Code style (mono uppercase header, hover row bg, subtle borders).
- Labels (INNER/OUTER, SINGLE/PARALLEL, CLIENT/SERVER, Q1–Q4, NIT…) = pill components.

## Structure

- **Cover:** key facts (title, id, section count, modules) — VS Code Welcome-page feel.
- **Body:** each numbered section of the input md → one slide.
- **TOC:** one-page summary of all sections; clicking jumps. Build jump targets from data (find slide index by id), never hardcode.
- **Closing:** thanks / Q&A.

## Slide head standard

- eyebrow: `§ NN · CATEGORY` (uppercase, mono, blue)
- title: large system-UI 600 weight; `<em>` = blue emphasis
- subtitle: one-line summary
- meta (top-right): file path + line number

## Auto-derived, never hardcoded

- Footer `X / N` count = `slides.length` (so adding a slide updates it everywhere).
- TOC entries + `#`-jump targets resolve by slide id at runtime.

---

## "Things I keep forgetting" checklist (what breaks if skipped)

| Item | If skipped |
|---|---|
| `pre` newline handling (`pre{white-space:normal}` + `.ln{white-space:pre}`) | blank lines between diff `+/-` rows |
| ASCII art → HTML grid/table | mono-width misalignment |
| Slide total count derived from data | footer `X / N` is wrong |
| Add the new slide to the TOC | new slide missing from contents |
| `#`-jump targets resolved by id | TOC jumps to the wrong slide |
| `step` spans joined with no source newline | spurious blank line in code |
| Playwright verification | ship a visual bug unseen |
| **Code block internal scroll** (`.code-wrap` + `min-height:0` chain) | zoomed code clipped at the bottom (don't paper over with page scroll) |
| **`min-height:0` on every scroll ancestor** | child won't shrink → clips instead of scrolling (most common) |
| **No `transform:scale` of the whole deck** | text shrinks — opposite of "zoom to read" |
| Right notes column width-capped/fluid | right column clipped when zoomed |
| **Footer = viewport `fixed; left:0; right:0`** | footer disappears or right end ("X/N") clipped when zoomed |
| Footer NOT `fixed+translateX+width:1440` | centered, both ends clipped when zoomed |
| Footer NOT `absolute` inside the scroll area | scrolls away with content |
| **Gradient fade = `rgba(color,0)`** (not `transparent`) | grey band / hard cut (banding) |
