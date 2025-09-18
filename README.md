# Phase 2 Pro Clean Bundle

This repo contains the complete Phase 2 Pro setup:
- server.js with admin + reversals + fingerprints + neighbors
- collector.js for Yahoo polling
- db/schema_phase2.sql with full schema
- render.yaml for Render deploy
- package.json with dependencies

## Deploy Steps
1. Push to GitHub
2. Deploy on Render with Blueprint (render.yaml)
3. Set ADMIN_KEY in Render Env Vars
4. Apply schema once via /admin/apply-schema
