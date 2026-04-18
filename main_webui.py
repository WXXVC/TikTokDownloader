from asyncio import CancelledError, run

from src.application import TikTokDownloader


async def main():
    async with TikTokDownloader() as downloader:
        try:
            downloader.project_info()
            downloader.check_config()
            await downloader.check_settings(False)
            # Container startup is non-interactive, so do not block on the disclaimer prompt.
            if not downloader.config.get("Disclaimer"):
                await downloader.database.update_config_data("Disclaimer", 1)
                downloader.config["Disclaimer"] = 1
            await downloader.web_ui()
        except (
            KeyboardInterrupt,
            CancelledError,
        ):
            return


if __name__ == "__main__":
    run(main())
