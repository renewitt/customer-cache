import setuptools

setuptools.setup(
    name="mpi",
    version="0.1",
    description="Maintain a list of current customer orders and publish the current list.",
    install_requires=[
        "amqp",
        "redis",
        "pyyaml"
    ],
    entry_points={
        'console_scripts': [
            'mpi = mpi.__main__:main'
        ],
    }
)
