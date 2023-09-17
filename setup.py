from setuptools import setup, find_packages

setup(
    name="streamback",
    version="0.0.1",
    author='Stefanos Liakis',
    author_email='stliakis@gmail.com',
    description="2-way streams for your microservices",
    packages=find_packages(),
    install_requires=[
        "redis>=2.10.6", "confluent-kafka>=1.7.0"
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
)
