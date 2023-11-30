from setuptools import setup, find_packages
import sys

with open('README.md', 'r') as fh:
    long_description = fh.read()

if sys.version_info >= (2, 7) and sys.version_info < (3,):
    REQUIRES = [
        "redis>=2.10.6", "confluent-kafka==1.7.0"
    ]
else:
    REQUIRES = [
        "redis>=2.10.6", "confluent-kafka>=1.7.0"
    ]

setup(
    name="streamback",
    version="0.0.35",
    author='Stefanos Liakis',
    author_email='stliakis@gmail.com',
    description="Two way streams for your microservices",
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=REQUIRES,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
)
