from setuptools import setup

setup(
    name="kuaipandriver",
    version='0.5',
    keywords = ["fuse", "kuaipan", "network driver", "mount"],
    license='MIT',
    description="A network driver's fuse Implementation base on kingsoft's kuaipan api",
    author='alex8224@gmail.com, birdaccp@gmail.com',
    author_email='alex8224@gmail.com, birdaccp@gmail.com',
    url='https://github.com/alex8224/fuse_for_kuaipan',
    packages=['kuaipandriver'],
    package_data={
        '': ['*.ini']
    },
    install_requires=[
        "requests>=2.3.0", "fusepy>=2.0.2","lxml>=3.3.5"]
    ,
    platforms = 'Linux',
    entry_points="""
    [console_scripts]
    mount.kuaipan = kuaipandriver.kuaipanfuse:main
    test.apidaylimit = kuaipandriver.kuaipanapi:test_api_limit
    test.docconvert= kuaipandriver.kuaipanapi:test_doc_convert
    test.fileupload= kuaipandriver.kuaipanapi:test_file_upload
    """,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities'
    ],
    long_description="A network driver's fuse Implemention base on kingsoft's kuaipan api",
)
