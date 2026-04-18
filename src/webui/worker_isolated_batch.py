import argparse
import asyncio
import json
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
            reconfigure(
                encoding="utf-8",
                errors="backslashreplace",
                line_buffering=True,
                write_through=True,
            )


def patch_project_root(volume_path: Path) -> None:
    import src.custom as custom
    import src.custom.internal as internal

    internal.PROJECT_ROOT = volume_path
    custom.PROJECT_ROOT = volume_path
    volume_path.mkdir(parents=True, exist_ok=True)


async def run_download(
    source_file: Path,
    platform: str,
    tab: str,
    sec_user_id: str,
    mark: str,
) -> None:
    from src.application.TikTokDownloader import TikTokDownloader
    from src.application.main_terminal import TikTok

    source_items = json.loads(source_file.read_text(encoding="utf-8"))
    async with TikTokDownloader() as downloader:
        downloader.check_config()
        await downloader.check_settings(False)
        app = TikTok(
            downloader.parameter,
            downloader.database,
            server_mode=True,
        )
        await app._batch_process_detail(
            source_items,
            tiktok=platform == "tiktok",
            mode=tab or "post",
            mark=mark or "",
            user_id=sec_user_id or "",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume", required=True)
    parser.add_argument("--platform", choices=("douyin", "tiktok"), required=True)
    parser.add_argument("--tab", required=True)
    parser.add_argument("--sec-user-id", required=True)
    parser.add_argument("--mark", default="")
    parser.add_argument("--source-file", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_stdio()
    patch_project_root(Path(args.volume))
    print(
        f"[worker] isolated batch starting, platform={args.platform}, tab={args.tab}, source={args.source_file}",
        flush=True,
    )
    try:
        asyncio.run(
            run_download(
                Path(args.source_file),
                args.platform,
                args.tab,
                args.sec_user_id,
                args.mark,
            )
        )
    except Exception as error:
        print(f"[worker] isolated batch crashed: {error!r}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    main()
