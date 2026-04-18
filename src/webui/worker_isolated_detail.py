import argparse
import asyncio
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
ENGINE_PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(ENGINE_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_PROJECT_ROOT))


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="backslashreplace")


def patch_project_root(volume_path: Path) -> None:
    import src.custom as custom
    import src.custom.internal as internal

    internal.PROJECT_ROOT = volume_path
    custom.PROJECT_ROOT = volume_path
    volume_path.mkdir(parents=True, exist_ok=True)


async def run_download(detail_ids: list[str], tiktok: bool) -> None:
    from src.application.TikTokDownloader import TikTokDownloader
    from src.application.main_terminal import TikTok

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
    parser.add_argument("--volume", required=True)
    parser.add_argument("--platform", choices=("douyin", "tiktok"), required=True)
    parser.add_argument("--ids", nargs="+", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_stdio()
    patch_project_root(Path(args.volume))
    asyncio.run(run_download(args.ids, args.platform == "tiktok"))


if __name__ == "__main__":
    main()
