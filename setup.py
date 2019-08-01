from setuptools import find_packages, setup


def readme():
    with open('README.md') as f:
        return f.read()


def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(
    name='redisbus',
    description='Minimal Redis based message bus model in Python.',
    long_description=readme(),
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
