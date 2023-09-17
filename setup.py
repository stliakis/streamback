from setuptools import setup, find_packages

install_requires = [
    r.strip() for r in open('requirements.txt')
    if r.strip() and not r.strip().startswith('#')
]

setup(
    name="streamback",
    version="0.0.1",
    author='Stefanos Liakis',
    author_email='stliakis@gmail.com',
    description="2-way streams for your microservices",
    packages=find_packages(),
    install_requires=[],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
)
