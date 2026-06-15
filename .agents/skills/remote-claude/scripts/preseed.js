// Seed claude start-gate flags (onboarding/theme/folder-trust) into ~/.claude.json so startup passes.
// dispatch runs this on the remote as `node ~/rc/preseed.js <repo-abs-path>`. Other keys are preserved.
const fs = require('fs');
const os = require('os');
const repo = process.argv[2] || (os.homedir() + '/dev/cubrid');
const p = os.homedir() + '/.claude.json';
let c = {};
try { c = JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) {}
c.hasCompletedOnboarding = true;            // skip the login-method choice screen
c.theme = c.theme || 'dark';
c.bypassPermissionsModeAccepted = true;     // best-effort: skip the bypass warning (ignored on versions without it)
c.projects = c.projects || {};
c.projects[repo] = c.projects[repo] || {};
c.projects[repo].hasTrustDialogAccepted = true;   // skip folder trust
fs.writeFileSync(p, JSON.stringify(c, null, 2));
console.log('preseed ok:', repo);
