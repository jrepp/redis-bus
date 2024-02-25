import os

from setuptools import find_packages, setup

LONG_DESC = """Components including worker scripts, stateless clients \
or a command line tool use redis to share messages reliably""" 


def local_path():
    filename = os.path.abspath(__file__)
    local_path = os.path.dirname(filename)
    return local_path


def readme():
    with open(os.path.join(local_path(), 'README.md')) as f:
        return f.read()


def requirements():
    return ['redis'] 


setup(
    name='redisbus',
    description='Simple message bus component library',
    long_description=LONG_DESC,
    keywords='redis message bus worker',
    url='https://github.com/jrepp/redisbus',
    license='MIT',
    version='0.8.0',
    platforms='Windows MacOS POSIX',
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements(),
    entry_points={
        'console_scripts': [
            'redisbus-cli=redisbus.cli:main'
        ]
    },
    author="Jacob Repp",
    author_email="jacobrepp@gmail.com",
    classifiers=[
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Environment :: Console',
        'Framework :: RedisBus',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
    ]
)
