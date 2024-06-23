from distutils.core import setup

setup(
    name="finviz-platform",
    packages=["finviz", "finviz.helper_functions", 
              "finviz_utils", "finviz_utils.earnings_calendar", 
              "finviz_utils.earnings_anomaly"],
    version="0.1.0",
    license="MIT",
    description="Finviz Scrapper with additional tools",
    author="Alberto Rincones",
    author_email="aa.rincones@gmail.com",
    url="https://github.com/c4road/finviz-platform",
    download_url="https://github.com/c4road/finviz/archive/v1.4.6.tar.gz",
    keywords=["finviz", "api", "screener", "finviz api", "charts", "scraper"],
    install_requires=[
        "wheel",
        "lxml",
        "requests",
        "aiohttp",
        "urllib3",
        "cssselect",
        "user_agent",
        "beautifulsoup4",
        "tqdm",
        "tenacity",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
    ]
)
