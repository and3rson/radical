import asyncio

from radical.decorators import method


@method
def add(a, b):
    return a + b

@method
async def wait(delay, result=42):
    return await asyncio.sleep(delay, result=result)
