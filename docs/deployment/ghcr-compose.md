# GHCR + Docker Compose Deployment

This guide targets the isolated containerization branch and keeps business source files untouched.

## 1) Build and push image from your workstation

Use your GitHub username and a Personal Access Token with package write permission.

1. Login to GHCR:

   echo <YOUR_GH_PAT> | docker login ghcr.io -u <YOUR_GITHUB_USERNAME> --password-stdin

2. Build image:

   docker build -t ghcr.io/nekyuuya/shinbot:latest .

3. Optional version tag:

   docker tag ghcr.io/nekyuuya/shinbot:latest ghcr.io/nekyuuya/shinbot:v0.1.1

4. Push tags:

   docker push ghcr.io/nekyuuya/shinbot:latest
   docker push ghcr.io/nekyuuya/shinbot:v0.1.1

Set the GHCR package visibility to public in GitHub package settings.

## 2) Run on test machine with Compose

Prepare files in one directory on the test machine:
- compose.ghcr.yml
- config.toml
- data/ (empty is fine for first run)

Start service:

   docker compose -f compose.ghcr.yml up -d

Check logs (first boot admin info may appear here):

   docker compose -f compose.ghcr.yml logs -f shinbot

Stop service:

   docker compose -f compose.ghcr.yml down

## 3) Upgrade and rollback

Upgrade to a fixed tag:

   SHINBOT_IMAGE=ghcr.io/nekyuuya/shinbot:v0.1.1 docker compose -f compose.ghcr.yml up -d

Rollback to previous tag:

   SHINBOT_IMAGE=ghcr.io/nekyuuya/shinbot:v0.1.0 docker compose -f compose.ghcr.yml up -d

## Notes

- Keep config.toml mounted from host; do not bake secrets into images.
- Persist data directory to retain database, sessions, and plugin data.
- If OneBot reverse mode is enabled, expose listener port in compose as needed.
