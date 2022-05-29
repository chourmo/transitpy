import threading
import asyncio
import aiohttp
import os

from .data_utils import zip_filename


class RunThread(threading.Thread):
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def run(self):
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))


def run_async(func, *args, **kwargs):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(func(*args, **kwargs))


async def _download(url: str, alternate_url: str, name: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                content = await resp.read()
                return content

            elif alternate_url is not None:
                async with session.get(alternate_url) as resp2:
                    if resp2.status == 200:
                        content = await resp2.read()
                        return content
                    else:
                        print("{0} is missing, error {1}".format(name, resp2.status))
                        return None
            else:
                print("{0} is missing, error {1}".format(name, resp.status))
                return None


async def _write(content: bytes, name: str, path: str) -> None:
    with open(os.path.join(path, zip_filename(name)), "wb") as f:
        if content is not None:
            f.write(content)


async def _get_feed(url, alternate_url, name, path) -> None:
    content = await _download(url, alternate_url, name)
    await _write(content, name, path)


async def _get_all_feeds(df, path, rename=None):

    if rename is None:
        d = {}
    else:
        d = rename

    tasks = [
        _get_feed(row.url, row.original_url, d.get(row.name, row.name), path)
        for row in df[["url", "original_url", "name"]].itertuples()
    ]
    return await asyncio.gather(*tasks)


class Datasource:
    """
    Abstract class for GTFS datasources

    Args :
        source_url : url to dowload a list of all resources
        name : string name of a column in data
        urls : list of columns with url to download data from

    Attributes :
        data : dataframe of resources
        missing_data : dataframe of resources not downloadable
    """

    def __init__(self, source_url, name, urls):
        self.source_url = source_url
        self.name = name
        self.urls = urls
        self.data = None
        self.missing_data = None

    def get_feeds(self, path, names=None):
        """
        get feeds from datasource and save to path

        args:
            path : directory to save into
            names : optional string, list or dict of names,
                if string or list of strings, subset of datasource name to download
                if dict, download keys and save as values
        """

        if names is None:
            df = self.data
            run_async(_get_all_feeds, df, path)

        elif isinstance(names, str):
            df = self.data.loc[self.data[self.name] == names]
            run_async(_get_all_feeds, df, path)

        elif isinstance(names, dict):
            df = self.data.loc[self.data[self.name].isin(names.keys())]
            run_async(_get_all_feeds, df, path, names)

        elif isinstance(names, list):
            df = self.data.loc[self.data[self.name].isin(names)]
            run_async(_get_all_feeds, df, path)

        else:
            raise ValueError("Names must be None, string, list or dict")

        return None
