from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name="streamback",
    version="0.0.13",
    author='Stefanos Liakis',
    author_email='stliakis@gmail.com',
    description="2-way streams for your microservices",
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=[
        "redis>=2.10.6", "confluent-kafka>=1.7.0"
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
)
