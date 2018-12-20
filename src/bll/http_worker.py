import logging

import requests

from src.bll.tools import retry
from src.config import config

import asyncio
#from pyppeteer.launcher import Launcher
from pyppeteer import launch


class HttpWorker:
    timeout = 30
    logger = logging.getLogger('{}.{}'.format(config.app_id, 'http'))
    cookies = {'ASP.NET_SessionId': 'ubn34qg3xsxiherq2sonh20q', 'MenuPage': '0'}
    current_open_selector = '#ctl00_RootContentPlaceHolder_MainPageControl_CurrentGridView_DXPagerBottom_DDB'
    current_select = '#ctl00_RootContentPlaceHolder_MainPageControl_CurrentGridView_DXPagerBottom_PSP_DXI5_T'
    arc_open_selector = '#ctl00_RootContentPlaceHolder_MainPageControl_ArchiveGridView_DXPagerBottom_DDB'
    arc_select = '#ctl00_RootContentPlaceHolder_MainPageControl_ArchiveGridView_DXPagerBottom_PSP_DXI4_T'

    @classmethod
    @retry(logger)
    def get_organization(cls, name, inn, kpp):
        result = {
            'guid': None,
            'name': name,
            'region': None
        }
        if inn is None or kpp is None:
            return result

        url = 'http://{}/organization?inn={}&kpp={}&name={}'.format(
            config.organizations_host, inn, kpp, name)
        headers = {'Authorization': config.organizations_token}
        r = requests.get(url, headers=headers)
        if r is None or r.text is None or r.status_code != 200:
            return result
        return r.json()

    @classmethod
    async def _js_render_tender_list(cls, arc):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(config.base_url, proxies=config.proxy)
        if arc:
            await page.click('#ctl00_RootContentPlaceHolder_MainPageControl_T1T')
        await page.waitForSelector(cls.arc_open_selector if arc else cls.current_open_selector)
        await page.click(cls.arc_open_selector if arc else cls.current_open_selector)
        await page.waitForSelector(cls.arc_select if arc else cls.current_select)
        await page.click(cls.arc_select if arc else cls.current_select)
        await page.waitFor(6000)
        #await page.screenshot({'path': 'example.png'})
        result = await page.content()
        await page.close()
        await browser.close()
        return result

    @classmethod
    async def _js_render_tender(cls, url):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(config.base_url + url, proxies=config.proxy)
        await page.waitForSelector(
            '#ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView_DXPagerBottom_DDB')
        await page.click(
            '#ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView_DXPagerBottom_DDB')
        await page.waitForSelector(
            '#ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView_DXPagerBottom_PSP_DXI6_T')
        await page.click(
            '#ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView_DXPagerBottom_PSP_DXI6_T')
        await page.waitFor(4000)
        result = await page.content()
        await page.close()
        await browser.close()
        return result

    @classmethod
    async def _js_lot_render(cls, url, length, index):
        if index == 0:
            # browser = await launch()
            browser = await launch(
                headless=True,
                args=['--no-sandbox'],
                autoClose=False
            )
            cls.browser_endpoint = browser.wsEndpoint
        else:
            browser = await connect({'browserWSEndpoint': cls.browser_endpoint})
        pages = await browser.pages()
        if len(pages) < 3:
            page = await browser.newPage()
        else:
            page = pages[0]
        await page.goto(url, proxies=config.proxy)
        # await page.screenshot({'path': 'example.png'})
        result = await page.content()
        await page.waitFor(4000)
        await page.close()
        if index + 1 == length:
            await browser.close()
        else:
            await browser.disconnect()
        return result

    @classmethod
    @retry(logger)
    def get_tenders_list(cls, arc=False):
        res = asyncio.get_event_loop().run_until_complete(cls._js_render_tender_list(arc))
        return res

    @classmethod
    @retry(logger)
    def get_tender(cls, tender_url):
        return requests.post(config.base_url + tender_url, cookies=cls.cookies, proxies=config.proxy)

    @classmethod
    @retry(logger)
    def get_render_tender(cls, tender_url):
        res = asyncio.get_event_loop().run_until_complete(cls._js_render_tender(config.base_url + tender_url))
        return res

    @classmethod
    @retry(logger)
    def get_lot(cls, lot_url, browser=True):
        if browser:
            res = asyncio.get_event_loop().run_until_complete(cls._js_lot_render(config.base_url + lot_url))
            return res
        else:
            res = requests.get(config.base_url + lot_url, proxies=config.proxy)
            return res
