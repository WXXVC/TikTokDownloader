import argparse
import asyncio
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
ENGINE_PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(ENGINE_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_PROJECT_ROOT))

from src.application.TikTokDownloader import TikTokDownloader
from src.application.main_terminal import TikTok


async def run_download(detail_ids: list[str], tiktok: bool) -> None:
    async with TikTokDownloader() as downloader:
        downloader.check_config()
        await downloader.check_settings(False)
        app = TikTok(
            downloader.parameter,
            downloader.database,
            server_mode=True,
        )
        root, params, logger = app.record.run(app.parameter)
        async with logger(root, console=app.console, **params) as record:
            await app._handle_detail(
                detail_ids,
                tiktok,
                record,
                api=False,
                source=False,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=("douyin", "tiktok"), required=True)
    parser.add_argument("--ids", nargs="+", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_download(args.ids, args.platform == "tiktok"))


if __name__ == "__main__":
    main()
