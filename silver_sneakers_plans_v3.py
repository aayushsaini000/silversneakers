import traceback
import re
import csv
import json
import time
import requests
import scrapy
from scrapy.crawler import CrawlerProcess

# PROXY = '37.48.118.90:13042'
PROXY = "45.79.220.141:3128"


def get_states():
    states_map = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "DC": "District Of Columbia",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "PR": "Puerto Rico",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VA": "Virginia",
        "VT": "Vermont",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming",
    }
    # states_map = {"IL": "Illinois"}
    # states_map = {"SC": "South Carolina"}
    return states_map


class ExtractItem(scrapy.Item):
    state = scrapy.Field()
    company = scrapy.Field()
    plans = scrapy.Field()


class SilverSneankerPlans(scrapy.Spider):
    name = "silver_sneakers_spider"
    allowed_domains = ["tools.silversneakers.com"]
    scraped_data = list()
    fieldnames = ['state', 'company', 'plans']

    def start_requests(self):
        base_url = "https://tools.silversneakers.com/Eligibility/GetHealthPlansByState?"
        states_map = get_states()
        for code, state in states_map.items():
            yield scrapy.Request(
                        url=base_url + f"state={code}",
                        callback=self.parse,
                        dont_filter=True,
                        meta={'state':state}
                    )

    def parse(self, response):
        if not response.status == 200:
            return
        state_group_divs = response.xpath("""//div[contains(@data-groups, '[" state-cluster"]')]""")
        for idx, state_div in enumerate(state_group_divs):
            company_name = state_div.xpath('a/img/@alt').extract_first().strip()
            plans_a_tags = state_div.xpath('div/div[1]/a')
            plans = ""
            for idx, atag in enumerate(plans_a_tags):
                plan_text = atag.xpath('text()').extract_first().strip()
                plans += f"{idx + 1}.{plan_text} "
            item = ExtractItem()
            item['state'] = response.meta['state']
            item['company'] = company_name
            item['plans'] = plans    
            yield item


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(SilverSneankerPlans)
    process.start()




if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
