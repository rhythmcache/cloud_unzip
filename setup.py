from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="cloud_unzip",
    version="0.1.3",
    author="rhythmcache",
    description="python script to extract files from remote ZIP archives without downloading the entire archive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rhythmcache/cloud_unzip",
    packages=find_packages(),
    install_requires=[
        'fsspec',
        'aiohttp',
        'requests',
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': [
            'cloud_unzip=cloud_unzip.core:main',
        ],
    },
)
