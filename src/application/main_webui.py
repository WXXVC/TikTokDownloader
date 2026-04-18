import os

from uvicorn import Config, Server

from ..custom import SERVER_HOST, SERVER_PORT

if False:  # pragma: no cover
    from ..config import Parameter
    from ..manager import Database


class WebUIServer:
    def __init__(self, parameter: "Parameter", database: "Database"):
        self.parameter = parameter
        self.database = database
        self.server = None

    async def run_server(
        self,
        host=SERVER_HOST,
        port=SERVER_PORT,
        log_level="info",
    ):
        os.environ.setdefault("DOUK_WEBUI_PORT", str(port))
        os.environ.setdefault("ENGINE_API_BASE", f"http://127.0.0.1:{port}/api")
        from src.webui.app import create_webui_app

        self.server = create_webui_app(self.parameter, self.database)
        config = Config(
            self.server,
            host=host,
            port=port,
            log_level=log_level,
        )
        server = Server(config)
        await server.serve()
