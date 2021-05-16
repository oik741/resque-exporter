from setuptools import setup, find_packages

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='resque-exporter',
    version='0.1.0',
    description='Resque Prometheus exporter',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/oik741/resque-exporter',
    author='Vladimir Shaykovskiy',
    author_email='oik741@gmail.com',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Monitoring',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='monitoring prometheus exporter resque',
    packages=find_packages(exclude=['tests']),
    python_requires='>=3.5',
    install_requires=[
        'prometheus-client==0.10.*',
        'redis',
    ],
    entry_points={
        'console_scripts': [
            'resque-exporter=resque_exporter:main',
        ],
    },
)
