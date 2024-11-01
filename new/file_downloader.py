from typing import IO, Optional
import aiohttp
import io


class AsyncMemoryFileManager:
    def __init__(self, url: str) -> None:
        self.url = url
        self.file_content: Optional[IO] = None

    async def __aenter__(self) -> IO:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                response.raise_for_status()
                self.file_content = io.BytesIO(await response.read())
                return self.file_content

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.file_content is not None:
            self.file_content.close()