// FAVE entry-point shim. The original ~16k-line main.js has been split into
// classical scripts under frontend/assets/js/{lib,models,views,controllers}.
// This file is kept so index.html's existing <script src="assets/js/main.js">
// tag stays valid. Load order is enforced in index.html. See CLAUDE.md.
console.info('[FAVE] main.js loaded — modules under assets/js/{lib,models,views,controllers}');
