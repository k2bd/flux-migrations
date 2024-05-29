import asyncio
import sys

# Unfortunate workaround for Windows compatibility
if sys.version_info >= (3, 8) and sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore  # noqa: E501
