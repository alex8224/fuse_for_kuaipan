from setuptools import setup, find_packages
setup(
    name = "kuaipandriver",
    version = "0.1",
    packages = find_packages(),
)


setup(
    name="kuaipandriver",
    version='0.1',
    license='MIT',
    description="A network driver's fuse Implementation base on kingsoft's kuaipan api",
    author='Alex8224',
    author_email='alex8224@gmail.com',
    url='https://github.com/alex8224/fuse_for_kuaipan',
    packages=['kuaipandriver'],
    package_data={
        'kuaipandriver': ['README.rst', 'LICENSE', 'config.ini']
    },
    install_requires=[
        "requests>=2.3.0", "fusepy>=2.0.2","lxml>=3.3.5"]
    ,
    entry_points="""
    [console_scripts]
    mount.kuaipan = kuaipandriver.kuaipanfuse:main
    test.apidaylimit = kuaipandriver.kuaipanapi:test_api_limit
    """,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: Network Driver',
    ],
    long_description="A network driver's fuse Implemention base on kingsoft's kuaipan api",
)
