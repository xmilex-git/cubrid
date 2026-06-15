# Verification recipe (Playwright MCP)

A **gate**, not a design loop. Assertions are **data-driven** — they iterate the actual `slides`/`DECK`, never hardcoded ids (a given deck won't have `s01`/`s08`). If something fails, you almost certainly edited the verified skeleton — diff against `skeleton.html` / `example-deck.html` and restore.

## Open it

`file://` works for headless checks. To open in a browser tab, serve with a **threaded** server (stock single-threaded `python3 -m http.server` hangs once a browser holds a keep-alive); on this host you can also host it in a detached `tmux` session so it survives:

```python
# serve.py  → tmux new-session -d -s deck "python3 serve.py"
import http.server, socketserver, os
os.chdir(os.path.expanduser("<deck folder>"))
class H(http.server.SimpleHTTPRequestHandler):
    def log_message(self,*a): pass
class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads=True; allow_reuse_address=True
S(("0.0.0.0", 8228), H).serve_forever()
```

`mcp__playwright__playwright_navigate` (chromium, headless). Change the viewport of an existing page with `mcp__playwright__playwright_resize` (navigate's width/height only apply to a fresh context).

## A) Zoomed-in gate — `playwright_resize` 1000×620, then walk EVERY slide+step

```js
(function(){
  var de=document.scrollingElement||document.documentElement;
  var f=document.getElementById('footer').getBoundingClientRect();
  var bad=[], sawOverflowScroll=false;
  for(var i=0;i<slides.length;i++){ cur=i; var ns=stepsOf(i);
    for(var s=0;s<ns;s++){ step=s; render();
      var pageBad = de.scrollWidth>de.clientWidth+1 || de.scrollHeight>de.clientHeight+1;
      var cw = slides[i].querySelector('.stepwrap[data-step="'+s+'"] .code-wrap') || slides[i].querySelector('.code-wrap');
      var codeUnderFooter=false;
      if(cw){ var r=cw.getBoundingClientRect();
        codeUnderFooter = r.bottom > f.top + 1;                 // code must never sit under the footer
        if(cw.scrollHeight > cw.clientHeight+1) sawOverflowScroll=true; // an overflowing panel scrolls internally
      }
      if(pageBad || codeUnderFooter) bad.push({id:slides[i].dataset.id, step:s, pageBad:pageBad, codeUnderFooter:codeUnderFooter});
    }
  }
  cur=0; step=0; render();
  return JSON.stringify({
    totalSlides: slides.length,
    footerFull: Math.round(f.left)===0 && Math.round(f.width)===innerWidth && Math.round(f.bottom)===innerHeight,
    sawInternalCodeScroll: sawOverflowScroll,   // informational (true if any code panel overflowed at this size)
    failures: bad                               // MUST be []
  });
})();
```
**Pass:** `failures` is `[]` and `footerFull` is `true`. (`sawInternalCodeScroll` is informational — it's only `true` when some code panel is tall enough to overflow at 1000×620; not required.)

## B) Full-size no-regression — `playwright_resize` 1440×900

```js
(function(){ var de=document.scrollingElement||document.documentElement, bad=[];
  for(var i=0;i<slides.length;i++){ cur=i; step=0; render();
    if(de.scrollWidth>de.clientWidth+1||de.scrollHeight>de.clientHeight+1) bad.push(slides[i].dataset.id); }
  cur=0;step=0;render();
  var f=document.getElementById('footer').getBoundingClientRect();
  return JSON.stringify({ pageScrollSlides:bad /* MUST be [] */, footerFull:Math.round(f.left)===0&&Math.round(f.width)===1440 });
})();
```

## C) Console errors = 0
Walking every slide+step in (A) already exercised all render paths. Then `mcp__playwright__playwright_console_logs` (type:"all") → **must be empty**.

## D) Screenshots (eyeball)
Capture, data-driven: `cover`, `toc`, `closing`, the **first** `DECK` slide, the first `DECK` slide whose right column contains a `table.vt`, and the first with a `.code-wrap`. Check: orange tab stripe + breadcrumb; **no blank line between diff `+/-` rows**; pills/tables styled; gradient continuous (no grey band/seam).

## Pass criteria
A `failures:[]` + `footerFull:true`; B `pageScrollSlides:[]`; C console empty; D looks right. Don't ship until all pass. A screenshot may come back downscaled (capture quirk) — confirm real size from the PNG IHDR, not the inline preview.
