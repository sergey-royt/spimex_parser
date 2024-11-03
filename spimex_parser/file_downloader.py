from typing import Optional
import aiohttp
from io import BytesIO


class AsyncMemoryFileManager:
    """
    Context manager which asynchronously download files
    and return BytesIO file object
    """

    def __init__(self, url: str) -> None:
        self.url: str = url
        self.file_content: Optional[BytesIO] = None

    async def __aenter__(self) -> BytesIO:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                response.raise_for_status()
                self.file_content = BytesIO(await response.read())
                return self.file_content

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Exception],
    ) -> None:
        if self.file_content is not None:
            self.file_content.close()
