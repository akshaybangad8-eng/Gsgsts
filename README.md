# Ujala Telegram Bot — GitHub-ready

## Files

```text
ujala_github_ready/
├── ujala_v699.py
├── ujala_pack.jpg
├── requirements.txt
├── README.md
├── .gitignore
└── .github/
    └── workflows/
        └── run-bot.yml
```

## Before running

The bot requires:

- `BOT_TOKEN` — Telegram bot token
- `ADMIN_IDS` — comma-separated Telegram numeric admin user IDs
- Required channel is already configured as `@thetricksmaster`.

Do **not** commit a real bot token to the repository.

## GitHub setup

1. Create a GitHub repository.
2. Upload all files while keeping the folder structure.
3. Open **Settings → Secrets and variables → Actions**.
4. Add repository secrets:
   - `BOT_TOKEN` = your Telegram bot token
   - `ADMIN_IDS` = your admin ID, e.g. `123456789`
     - Multiple admins: `123456789,987654321`
5. Open **Actions → Ujala Telegram Bot → Run workflow**.

## Important

`ujala_pack.jpg` must stay in the same directory as `ujala_v699.py`. The Python script resolves the image path relative to its own file, so the GitHub working directory does not matter.

The included `ujala_pack.jpg` is a 115×115 crop of the Ujala pack thumbnail from the screenshot supplied in the conversation. If you have the original/full-resolution pack image, replace this file with it and keep the filename exactly `ujala_pack.jpg`.

## GitHub Actions limitation

GitHub Actions is designed for CI/CD rather than permanent bot hosting. This workflow has a 350-minute job limit and is scheduled every 6 hours. For genuinely continuous 24/7 polling, use a persistent host such as a VPS or another long-running service.
