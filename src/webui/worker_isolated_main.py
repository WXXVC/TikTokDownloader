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


async def run_downloader() -> None:
    from src.application import TikTokDownloader

    async with TikTokDownloader() as downloader:
        await downloader.run()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_stdio()
    patch_project_root(Path(args.volume))
    print(f"[worker] isolated main starting, volume={args.volume}", flush=True)
    try:
        asyncio.run(run_downloader())
    except Exception as error:
        print(f"[worker] isolated main crashed: {error!r}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    main()
