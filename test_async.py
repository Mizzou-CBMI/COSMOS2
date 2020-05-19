import asyncio


async def f(x):
    return f"{x}"


async def async_map(func, *iterables):
    return asyncio.gather(*(func(*params) for params in zip(iterables)))


rv = asyncio.run(async_map(f, range(10)))
print(rv)
